---
title: "fix: BrokerPollerBackpressureTest vacuous first await (unsatisfiable shards-drained condition)"
type: fix
status: active
date: 2026-07-31
branch: fix/brokerpoller-backpressure-vacuous-await
---

# fix: BrokerPollerBackpressureTest vacuous first await (unsatisfiable shards-drained condition)

## Overview

`BrokerPollerBackpressureTest.brokerPollPausedWithEmptyShardsButHighInFlight` failed on the highcpu
lane (run 30603617471, 2026-07-31) with a 10s Awaitility timeout on its FIRST await. The inflight
ledger entry demanded classification before any timeout tweaking. Diagnosis verdict: **test-design
bug, not a main-code backpressure wedge, and not a duplicate of the known
tight-absolute-timeout-under-contention pattern**. The first await's condition is *vacuously true* on
quiet boxes (it evaluates before any records arrive) and *unsatisfiable* once records have arrived -
no timeout increase can fix it. This plan rewrites the test to await the real steady state, documents
the finding as a solutions doc, and closes the ledger entry.

## Problem Frame

The test configures `maxConcurrency(10)` + `messageBufferSize(150)`, produces 200 records with 200
unique keys (KEY ordering → 200 single-record shards), blocks all workers on a latch, then:

```java
await().atMost(10s).until(() -> pc.getWm().getNumberOfWorkQueuedInShardsAwaitingSelection() == 0);
```

**Why the condition is unsatisfiable after records arrive:**

- `messageBufferSize=150` pins the load factor statically:
  `PCModule.initDynamicLoadFactor()` → `DynamicLoadFactor(15, 15)` (150 / 10 = 15, no remainder).
- The control loop takes work only up to `target = getPoolLoadTarget() × factor = 10 × 15 = 150`
  (`AbstractParallelEoSStreamProcessor.calculateQuantityToRequest`, `getQueueTargetLoaded`).
- All 10 workers are latch-blocked, so `numberRecordsOutForProcessing` never decrements. It caps at
  150, leaving **50 records permanently in the shards**. `awaitingSelection` floors at 50, never 0.

**Why the test usually passes anyway (the vacuous window):**

- The await starts right after `pc.poll(...)` registration, but partition assignment + first fetch
  take ~1-2s. Until records are registered, the shard map is empty and
  `getNumberOfWorkQueuedInShardsAwaitingSelection()` returns **0 vacuously** - the await passes on
  its first check without testing anything.
- The subsequent `pausedPartitionSize == 1` assertion then passes for a *different* reason than the
  test name claims: buffer-full pause (`isSufficientlyLoaded`: 150 out + 50 in shards = 200 > 150),
  not "empty shards but high in-flight".

**Why it failed under load (run 30603617471):**

- The box was concurrently running Integration (8 broker-forks), Unit (32 forks), two PIT sweeps, and
  Performance. Setup/produce latency pushed the await's start to ~04:16:25.6 - *after* partition
  assignment (04:16:25.126 in the job log) and record registration. First check saw ~50-200 in
  shards; the floor of 50 made the timeout **deterministic** from that point. Elapsed 12.59s = ~2.6s
  setup + full 10s timeout.

**Proof the green runs are vacuous, not fast:** the gating GitHub-hosted Integration run on the SAME
head sha (run 30603617430, job 91071393783) passed the whole 2-test class in 5.518s - including two
deliberate 1s sleeps. A non-vacuous first-await pass is arithmetically impossible (50-record floor),
so every green pass rides the vacuous window.

**Structural corollary:** the scenario named by the test - poll paused while shards are empty and
in-flight is high - is *unreachable by design* with a static buffer: pause requires
`(inShards + outForProcessing) > 150`, but empty shards means the sum is `outForProcessing ≤ 150`.
The take-cap and the pause threshold are the same number. The test can only verify the adjacent,
real behaviour: in-flight records count toward backpressure (upstream #836's fix), i.e. poll stays
paused when shards hold only 50 (< 150) because the 150 blocked in-flight records are counted.

## Duplicate-Finding Assessment

- **Not a duplicate** of
  `docs/solutions/test-flakiness/parallel-integration-tests-flaky-under-concurrency-2026-07-28.md`
  (tight absolute timeouts under contention). Related family - load exposed it - but distinct root
  cause: hardening/loosening the 10s bound would NOT fix this test; the awaited condition is
  unsatisfiable post-arrival. Classifying it as "contention sensitivity, harden bounds" would have
  masked the real defect.
- **Not tracked anywhere else**: no GitHub issue in fork or upstream (`confluentinc/parallel-consumer`)
  mentions the test; not in the quarantine lane; not covered by #63, `fix/flaky-ensure-topic-timeout`,
  or the PartitionStateCommittedOffsetIT plan.
- **Not a main-code bug**: the 50-record shard floor is intended behaviour of the static buffer cap;
  pause/resume machinery behaved per design (sibling test passed in the same failing run).
- Test inherited from upstream (#682 configurable buffer size, #836 buffer/in-flight backpressure
  calculation fix) - the latent race shipped with it.

## Requirements Trace

- R1. First await must wait on a **satisfiable, meaningful** steady-state condition (no vacuous pass).
- R2. Test must still verify #836's behaviour: blocked in-flight records count toward poll pause.
- R3. Test must be robust on a heavily loaded box (operator stance: busy boxes must not cause
  failures) without weakening any assertion.
- R4. Finding recorded as a solutions doc; inflight ledger entry closed with a pointer.

## Scope Boundaries

- No main-code changes. The static-load-factor cap and pause threshold are working as designed.
- No change to the sibling test `brokerPollPausedWithHighNumberInShardsButLowInFlight` - its first
  await (`awaitingSelection == 195`) is a satisfiable steady state and it passed under the same load.
- No quarantining - the fix is small and removes the flake class entirely.

### Deferred to Separate Tasks

- None. The `docs/inflight.md` ledger entry (merged to master via the chaos W4 PR) is updated with
  the diagnosis and this branch name in the same commit as this plan; it gets marked fully resolved
  when the fix PR merges.

## Context & Research

### Relevant Code and Patterns

- `parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/BrokerPollerBackpressureTest.java`
  - the failing test (line 67 = the vacuous await).
- `parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/internal/PCModule.java` -
  `initDynamicLoadFactor()`: `messageBufferSize` → static factor 15/15.
- `parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/internal/AbstractParallelEoSStreamProcessor.java`
  - `calculateQuantityToRequest`, `getQueueTargetLoaded` (take-cap = 150).
- `parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/state/WorkManager.java` -
  `isSufficientlyLoaded()` (pause threshold = same 150, counts in-flight per #836).
- `parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/state/ProcessingShard.java` -
  `availableWorkContainerCnt` semantics (decremented on take, so "awaiting selection" ≠ "arrived").

### Evidence

- Failing run: highcpu 30603617471, job 91071293663 (Integration, forkCount=8), sha `8504817f`,
  branch `feats/chaos-pain-suite`. Timeline: PC init 04:16:24.18, produce 04:16:24.18, assignment
  04:16:25.126, `ConditionTimeoutException` at line 67 after 12.59s.
- Green run, same sha: CI 30603617430, job 91071393783 - class total 5.518s (vacuous-window pass).

### Institutional Learnings

- `docs/solutions/test-flakiness/parallel-integration-tests-flaky-under-concurrency-2026-07-28.md` -
  the adjacent-but-distinct pattern; its "classify before loosening" discipline is what caught this.
- AGENTS.md stress-failure discipline: stress failures may be real bugs - classify (contention vs
  bug) before touching timeouts. This case is the third category: **test-design bug** (vacuous
  condition), fixable only by rewriting the condition.

## Key Technical Decisions

- **Await the real steady state, not zero**: wait until `numberRecordsOutForProcessing == 150` AND
  `awaitingSelection == 50` (both derived from constants, not magic numbers). This is satisfiable,
  deterministic, and proves records arrived AND the take-cap engaged. Asserting the 50-floor also
  pins the static-buffer cap behaviour as a regression guard.
- **Keep verifying #836 semantics**: after steady state, assert poll is paused. Pause fires only
  because the 150 blocked in-flight records count toward `isSufficientlyLoaded` (50 alone < 150) -
  exactly the behaviour #836 introduced.
- **Rename the test** to describe what it actually verifies (e.g.
  `brokerPollPausedWhenBufferFullWithBlockedInFlight`), with javadoc explaining why
  "paused with empty shards" is unreachable by design (take-cap == pause threshold). A name that
  states an impossible scenario invites the next reader to "fix" the test back into vacuity.
  Upstream-sync friction is minimal: the fork already diverges in this file (Awaitility de-shading,
  copyright headers).
- **Generous await bounds**: keep/raise `atMost` to 30s on the awaits. With a satisfiable condition
  this costs nothing when green and absorbs busy-box latency (operator stance). This is not
  assertion-weakening: conditions get stronger, only the wall-clock allowance grows.

## Open Questions

### Resolved During Planning

- Contention flake or real bug?: **Neither dismissible** - test-design bug (vacuous await), proven by
  cap arithmetic + same-sha green/red run comparison.
- Duplicate of a known issue?: **No** - see Duplicate-Finding Assessment.
- Does a slow path in `getNumberOfWorkQueuedInShardsAwaitingSelection` mask a real backpressure
  wedge? (inflight entry branch (b)): **No wedge** - the "slow path" is a permanent floor of 50 by
  design; the sibling test proves pause/resume works under the same load.

### Deferred to Implementation

- Exact awaited-accessor choice (`pc.getWm().getNumberRecordsOutForProcessing()` is already used by
  the test at line 75, so no new surface is expected) - confirm no additional getter is needed.
- Whether the final resume/`210` awaits also want the 30s bound - decide while touching the file.

## Implementation Units

- [x] **Unit 1: Rewrite the vacuous await and rename the test** *(done 2026-07-31: RED reproduced
  deterministically first try on a quiet box - arrival-synced original await timed out in exactly
  10s, per prediction; rewritten test green 2/2 runs)*

**Goal:** Replace the unsatisfiable `awaitingSelection == 0` await with the real steady-state
condition; rename test + add explanatory javadoc; derive 150/50 from named constants.

**Requirements:** R1, R2, R3

**Dependencies:** None. Branch off `master` (per PR-isolation practice), not off a chaos branch.

**Files:**
- Modify: `parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/BrokerPollerBackpressureTest.java`

**Execution note:** Characterization-first - before rewriting, build the deterministic RED
reproducer: keep the original `awaitingSelection == 0` await but precede it with an arrival-sync
(await `awaitingSelection > 0`). Per the diagnosis this must time out every run with no load. If it
passes, the cap analysis is wrong - stop and re-diagnose. Capture the RED output for the PR and
solutions doc; do not commit the reproducer as a suite test (it proves a negative in 10s+). The
arrival-sync step then stays in the rewritten test - it is what makes the final version race-free.

**Approach:**
- First await → steady state: out-for-processing == `MESSAGE_BUFFER_SIZE` (150) and
  awaiting-selection == `MESSAGE_COUNT - MESSAGE_BUFFER_SIZE` (50), `atMost(30s)`.
- Then the existing pause assertion chain (paused == 1, 1s stability sleep, still paused) stands,
  now non-vacuously reached.
- Javadoc: state the take-cap == pause-threshold identity and that "paused with empty shards" is
  unreachable; reference upstream #836 and the failing run ID for archaeology.

**Test scenarios:**
- Happy path: 200 unique-key records, workers latch-blocked → steady state reached (150 out / 50
  awaiting) → poll paused; stays paused through 1s stability window.
- Happy path (existing tail, unchanged assertions): latch release → all 200 processed, 0 out for
  processing → poll resumes → 10 further records processed (proves resumption actually polls).
- Regression guard: awaiting-selection settles at exactly 50 while blocked - pins the static-buffer
  take-cap; if main code ever lets shards drain below the floor while blocked, this fails loudly.
- Load robustness: conditions are state-convergent (no ordering race with record arrival), so
  busy-box latency only consumes await budget, never flips the outcome.

**Verification:**
- Test passes repeatedly locally (including a `-DforkCount=8` style loaded run or repeated
  invocations) and the full integration suite stays green.
- Reverting just the await change (back to `== 0`) with records forced to arrive first reproduces
  the deterministic timeout - confirming the rewrite addressed the actual mechanism.

- [x] **Unit 2: Record the finding as a solutions doc** *(done 2026-07-31:
  `docs/solutions/test-flakiness/vacuous-await-condition-brokerpoller-backpressure-2026-07-31.md`)*

**Goal:** Compound the lesson: "vacuously-true await conditions" as a distinct flaky-test class,
separate from tight-timeout contention.

**Requirements:** R4

**Dependencies:** Unit 1 (reference its fix).

**Files:**
- Create: `docs/solutions/test-flakiness/vacuous-await-condition-brokerpoller-backpressure-2026-07-31.md`

**Approach:**
- Follow the existing solutions-doc frontmatter format (symptoms / root_cause / resolution_type).
- Key content: the vacuous-truth mechanism, the same-sha green/red proof technique (green pass
  arithmetically impossible → must be vacuous), and the classification discipline that caught it.
  Cross-link the 2026-07-28 contention doc as the adjacent-but-different pattern.
- Prevention rule for the doc: an Awaitility condition of the form `X == 0` where `X` starts at 0
  and only later becomes non-zero is a red flag - await the non-zero intermediate state first, or
  await a convergent steady state instead.

**Test expectation:** none - documentation only.

**Verification:**
- Doc discoverable via `docs/solutions/test-flakiness/` listing; frontmatter matches house style.

## System-Wide Impact

- **Interaction graph:** test-only change; no production code touched.
- **API surface parity:** none - uses accessors the test already calls.
- **Integration coverage:** unchanged tail assertions keep end-to-end pause→resume→re-poll coverage.
- **Unchanged invariants:** static-buffer cap (`messageBufferSize` → fixed factor), pause threshold
  arithmetic, sibling test - all untouched; the rewrite *pins* the cap rather than altering it.

## Risks & Dependencies

| Risk | Mitigation |
|------|------------|
| Steady-state numbers (150/50) drift if main-code cap semantics change | Derive from named constants tied to the options values; regression-guard assertion fails loudly and points here |
| Rename creates upstream cherry-pick friction | File already fork-diverged; javadoc notes the upstream test name for mapping |
| Another hidden race in the remaining awaits under extreme load | All remaining conditions are convergent states with 30s budgets; solutions doc records the classification method if one surfaces |

## Sources & References

- Failing run: `gh run view 30603617471` (highcpu, Integration job 91071293663), sha `8504817f`
- Green same-sha run: `gh run view 30603617430` (CI, Integration job 91071393783)
- Inflight ledger entry: `docs/inflight.md` ("CI reliability / gate issues" section; updated with this diagnosis on this branch)
- Upstream provenance: confluentinc/parallel-consumer #682, #836
- Related solutions doc: `docs/solutions/test-flakiness/parallel-integration-tests-flaky-under-concurrency-2026-07-28.md`
