---
title: "fix: Drain-path zombie-spin fix (test-first) + uber-branch stall experiment"
type: fix
status: done
date: 2026-07-29
---

# fix: Drain-path zombie-spin fix (test-first) + uber-branch stall experiment

> **Done (landed in PR #80).** The drain-path zombie/busy-spin fix shipped, guarded by
> `BrokerPollSystemDrainTest` (RED→GREEN) and `DrainingMemberRebalanceIT`. Kept as diary history;
> the durable root-cause write-ups live in `docs/solutions/test-flakiness/`
> (`pc-silent-stall-under-contention-2026-07-29.md` and
> `latest-reset-nudge-race-committedoffsetremoved-2026-07-30.md`).

## Context

The PR #80 investigation root-caused a drain-path defect: `BrokerPollSystem.drain()` calls
`ConsumerManager.signalStop()` *before* entering `DRAINING`, so `ConsumerManager.poll()`'s
`while (!shutdownRequested.get())` guard means **`consumer.poll()` is never invoked again** during the
drain window. Consequences (measured): the poll loop busy-spins (~10,300 iterations/s, 92,880 polls in
9.0s) and the draining consumer becomes a **zombie group member** — background heartbeats keep it alive
and holding its full partition assignment, but it cannot participate in rebalances (rejoin/revoke-ack live
inside `poll()`), starving same-group siblings for up to `max.poll.interval.ms` (5 min). This maps onto
upstream #857's "paused consumption after rebalance". Verified: neither PR #29 (#857) nor PR #31 (#909)
fixes this — they fix sibling, non-conflicting mechanisms.

User-directed sequencing: **(A)** land focused test coverage of the theory on the PR #80 branch → commit;
**(B)** land the minimal fix → commit; **(C)** build the uber branch(es) merging #29 + #31 and measure
whether the combined fixes eliminate the residual stalls.

**Target repo/branch:** `astubbs/parallel-consumer`, branch `fix/flaky-partitionstate-committedoffset-it`
(PR #80, stacked on #75), worktree `.claude/worktrees/fix-flaky-partitionstate`.

## Requirements Trace

- R1. Deterministic unit-level test coverage that observably captures the defect (characterization first,
  per repo discipline: never commit a failing test).
- R2. Minimal drain fix: honour the paused 2s long poll as the drain-loop sleep (the code's own stated
  intent in `handlePoll()`), keeping the drainer rebalance-responsive; test from R1 flips to guard it.
- R3. Uber experiment: A/B-measure the marginal effect of the drain fix with #29 + #31 merged in, using
  (a) #29's chaos harness `MultiInstanceRebalanceTest#largeNumberOfInstances` (their residual: 10-20%
  stalls) and (b) this investigation's `forkCount=16` stress recipe.
- R4. Results recorded durably (report + inflight) whatever the outcome.

## Scope Boundaries

- NOT touching `PartitionStateCommittedOffsetIT`'s await (no masking — unchanged from PR #80's stance).
- NOT merging the uber branches anywhere or opening PRs from them — throwaway experiment branches.
- NOT attempting the deeper fail-safe drain options (hard deadline → explicit LeaveGroup; eager leave) —
  documented in the report's design review, deferred to the #857 stream.
- NOT fixing #29's pom conflict "properly" — resolve minimally for the experiment only.

## Context & Research (session-verified facts)

- Fix site: `parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/internal/BrokerPollSystem.java`
  — `drain()` L~225 calls `consumerManager.signalStop()`; `transitionToClosing()` L~295 **already calls
  `signalStop()`**, so the fix is deleting the call in `drain()` (plus comment). `handlePoll()`'s comment
  ("if draining - subs will be paused, so use this to just sleep") states the design intent.
- `shutdownRequested` blast radius (`ConsumerManager`): poll guard L77; commitSync retry loop L149
  (`tryCount == 0 ||` — still gets first attempt); SASL `retryBackOff` abort L217; `close()` sets it
  directly. Deferring the flag to CLOSING means drain-time commits/retries behave *normally* — arguably
  fixing a second subtle drain bug (commit retries aborting during drain).
- Close-latency check needed at implementation: confirm the CLOSING transition wakes the consumer
  (`consumerManager.wakeup()` guarded at L~342) so the blocking 2s poll doesn't add shutdown latency.
- Test infra (reuse, DRY): `AbstractParallelEoSStreamProcessorTestBase` builds a PC over a Mockito
  **spy** of `LongPollingMockConsumer` (`consumerSpy`, realistic blocking poll) — poll invocations are
  countable via Mockito. No existing `BrokerPollSystem`/drain unit test exists (only `PCModuleTestEnv`
  references it) → new test class is justified.
- Merge dry-runs (verified with `git merge-tree --write-tree`):
  - #29 (`bugs/857-paused-consumption-multi-consumers-bug`) into PR #80 branch → **1 conflict: `pom.xml`
    only** (#857 adds ~7 lines of test deps).
  - #31 (`fix/909-stale-container-replacement`) → **clean**.
- Chaos harness (on #29's branch; `@Disabled` on master): `MultiInstanceRebalanceTest#largeNumberOfInstances`
  — 12 PCs, 80 partitions, 500k messages, chaos monkey stop/start every 0-500ms, 5-min await, fails on
  11s no-progress. `@Tag("performance")` → excluded by default; run with
  `-Dincluded.groups=performance -Dit.test='MultiInstanceRebalanceTest#largeNumberOfInstances'`.
  #29's own acceptance note: ~90% pass, residual ~10% attributed to "Kafka behaviour under churn" — our
  zombie-drain theory is the competing explanation (chaos stop/start = drain-window factory), which the
  A/B tests directly.
- Report to update with results:
  `docs/solutions/test-flakiness/pc-silent-stall-under-contention-2026-07-29.md`.

## Key Technical Decisions

- **Characterization-first, two commits (user-directed).** Commit A: test asserting the *current defective
  behaviour* (0 `consumer.poll()` invocations during a drain window) — passes on current code, javadoc
  links the report. Commit B: the fix + flip the same test to the desired property (≥1 poll during
  window). The commit-B diff then *is* the behaviour change, reviewable at a glance. Never commits red.
- **Fix depth (user-selected): collapse the duplicated lifecycle state, not just re-time it.**
  `ConsumerManager.shutdownRequested` is a private shadow of `BrokerPollSystem.runState`, settable out of
  phase — that desync IS the bug. Commit B deletes the flag entirely: `ConsumerManager`'s three uses (poll
  guard, SASL `retryBackOff` abort, commitSync retry abort) consult an injected `BooleanSupplier
  abortSignal` derived from the poller's real lifecycle (`runState == CLOSING || CLOSED`). `signalStop()`
  and its call sites in `drain()`/`transitionToClosing()` are removed. One source of truth — the bug
  *class* becomes structurally impossible, per the "collapse parallel state when bugs recur" rule. The
  single-thread poll+control merge (which would eliminate the drain dance altogether) stays deferred to
  the #857 stream; the report's design review gains it explicitly as the end-state option.
- **Poll-invocation count is the single discriminating observable.** Defect: loop spins but
  `consumer.poll()` count stays 0 (short-circuit). Fixed: count advances ~1 per long-poll period and is
  bounded above (no spin through the poll path). One counter tests both spin and rebalance-responsiveness
  (poll is the rebalance-participation mechanism).
- **A/B arms isolate the drain fix's marginal effect.** Both arms = current PR #80 base + #29 + #31
  merged; arms differ *only* by the drain-fix commit (branch off `HEAD` vs `HEAD~1`... concretely: off the
  fix commit vs its parent). #29's 10-20% residual claim becomes measurable against an identical base.
- **CHANGELOG**: the fix is operator-visible (a core burned + partitions held during every close) → add an
  entry to the project changelog (`CHANGELOG.adoc` per repo convention — verify filename) in commit B.

## Open Questions

### Resolved During Planning
- Failing-test-commit vs characterization: characterization (repo rule: tests must pass at every commit).
- Fix mechanics: delete `signalStop()` from `drain()`; `transitionToClosing()` already covers CLOSING.
- Conflict surface for uber: `pom.xml` only.

### Deferred to Implementation
- Exact upper bound for the post-fix poll-count assertion (depends on mock long-poll period; pick with
  margin, e.g. window/period × 4 — must not be load-sensitive).
- Whether `transitionToClosing` needs a `wakeup()` added (only if close-latency verification shows +2s).
- Chaos-run count per arm if early results are lopsided (start 5/arm; stop early if 0-for-5 vs 5-for-5).

## Implementation Units

- [ ] **Unit 1 (Commit A): characterization test of the drain defect**

**Files:**
- Create: `parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/internal/BrokerPollSystemDrainTest.java`

**Approach:** extend `AbstractParallelEoSStreamProcessorTestBase`. Start PC over `consumerSpy`; feed
records; user function parks on a latch (work in-flight). Call `pc.close(DrainingMode.DRAIN)` on a
separate thread. During the drain window: snapshot `consumerSpy` poll-invocation count, wait ~1.5s,
snapshot again. Assert **delta == 0** (characterizes the `signalStop` short-circuit; javadoc links the
report + names the desired behaviour). Release latch; assert close completes cleanly.

**Execution note:** characterization-first — capture the defect as-is; do NOT fix in this commit.

**Test scenarios:**
- Happy path (sanity): before drain, poll count advances (proves the counter observable works).
- Characterization: during drain with in-flight work, poll delta == 0.
- Edge: latch released → close completes, no hang, no exception.

**Verification:** new test green on current code; full unit suite green; commit + push (updates PR #80).

- [ ] **Unit 2 (Commit B): collapse the duplicated lifecycle flag, test flipped to guard it**

**Files:**
- Modify: `parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/internal/ConsumerManager.java`
  (delete `shutdownRequested` + `signalStop()`; poll guard, SASL `retryBackOff` abort, and commitSync
  retry abort consult an injected `BooleanSupplier abortSignal`; `close()` keeps its local deadline)
- Modify: `parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/internal/BrokerPollSystem.java`
  (remove both `signalStop()` calls; expose the lifecycle predicate, e.g. `isTransitioningToClose()` =
  `runState == CLOSING || CLOSED`; wire it into `ConsumerManager` — comment explains the
  2s-poll-as-sleep intent + report link)
- Possibly touch: `PCModule.java` (wiring — see circularity note below)
- Modify: `BrokerPollSystemDrainTest.java` (assert delta ≥ 1 and ≤ bound; rename test to the property,
  e.g. drain keeps polling / stays rebalance-responsive)
- Modify: project changelog (operator-visible fix entry)
- Modify: `docs/solutions/test-flakiness/pc-silent-stall-under-contention-2026-07-29.md` + `docs/inflight.md`
  (status: state-collapse fix IMPLEMENTED on PR #80; design review gains the single-thread merge as the
  explicit end-state option, deferred to #857)

**Approach:** verify RED first (flipped test fails on unfixed code), then apply the collapse, then GREEN.
Wiring circularity: `PCModule` builds `ConsumerManager` before `BrokerPollSystem` exists — prefer setter
injection from `BrokerPollSystem`'s constructor (`consumerManager.setAbortSignal(this::isTransitioningToClose)`)
over restructuring the DI graph; keep it wired through the module's construction path (Use-DI rule).
Sequencing check at implementation: `doClose()` calls `consumerManager.close()` while `runState` is still
CLOSING — the supplier must already be true there (it is: CLOSING qualifies). Check close-latency:
existing close/drain tests' duration unchanged (±2s tolerance); add `wakeup()` on the CLOSING transition
only if needed.

**Test scenarios:**
- Happy path: during drain, poll delta in [1, bound] (long-poll cadence, no spin).
- Regression: full unit suite (incl. existing close/shutdown tests) green, no duration regressions.
- Integration spot-check: one `forkCount=16` recipe run — expect drain-spin signature gone from captures
  (no 10kHz `Got 0 records` bursts in DRAINING); stalls may still occur (other mechanisms) — do not
  over-claim.

**Verification:** RED→GREEN demonstrated; suites green; commit + push (updates PR #80).

- [ ] **Unit 3: build the two uber arms**

**Files:** no repo-file edits except `pom.xml` conflict resolution during merges (experiment branches only).

**Approach:**
- `experiment/stall-uber-nofix` — worktree branched from **commit A** (pre-fix) → merge
  `bugs/857-paused-consumption-multi-consumers-bug` (resolve `pom.xml`: take both sides) → merge
  `fix/909-stale-container-replacement`.
- `experiment/stall-uber-fix` — same, branched from **commit B** (with fix).
- Sanity per arm: compile + core unit suite green. Local only; never pushed as PRs.

**Test expectation:** none — branch assembly; sanity = existing suites.

**Verification:** both arms build; `git log` shows expected composition; unit suite green on both.

- [ ] **Unit 4: run the experiment + record results**

**Approach (per arm, local 12-core Mac, Docker up, runs sequential):**
- Chaos: 5 × `MultiInstanceRebalanceTest#largeNumberOfInstances`
  (`-Dincluded.groups=performance -Dit.test=...`, ~5-7 min each). Record pass/fail + failure mode
  (assignedPartitions=0 vs no-progress vs other); on failure, capture reports + our detectors' DEBUG.
- Stress: 3 × the `forkCount=16` full-core-IT recipe (~5 min each). Record stall incidence + which test.
- Compare arms. Prediction on record: if zombie-drain explains #29's residual, `uber-fix` chaos failures
  drop to ~0 while `uber-nofix` shows the 10-20% residual.

**Files:**
- Modify: `docs/solutions/test-flakiness/pc-silent-stall-under-contention-2026-07-29.md` — new
  "Uber-branch experiment results" section (run matrix, numbers, verdict, honest caveats — 5 runs is a
  small sample; state confidence accordingly).
- Modify: `docs/inflight.md` — experiment outcome one-liner.

**Test expectation:** none — the experiment IS the test; results committed as docs on the PR #80 branch.

**Verification:** results section complete with raw counts; committed + pushed to PR #80; uber worktrees
cleaned up (branches kept locally for reproduction).

## System-Wide Impact

- **Behaviour change (commit B):** during DRAINING the consumer now genuinely long-polls (paused → returns
  empty after ≤2s) instead of spinning; commits/SASL-retries during drain no longer aborted by the flag.
  Shutdown latency may grow by ≤1 poll timeout (2s) — measured by existing close tests; mitigate with
  wakeup on CLOSING if needed.
- **Unchanged invariants:** `DONT_DRAIN` close path (calls `transitionToClosing()` directly) unaffected;
  `PartitionStateCommittedOffsetIT` untouched; PR #75 workflow untouched; diagnostics from PR #80 remain.
- **Interaction risk:** rebalance-during-drain now *executes* revoke callbacks (previously the member was
  protocol-absent) — this is the desired behaviour but touches #29's CME/commit-lock territory; the uber
  arms (which include #29's fixes) are exactly where that composition gets exercised.

## Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Characterization test is timing-flaky (window too short/long) | Use invocation *count deltas* not timing asserts; generous window; mock consumer is deterministic |
| Fix regresses shutdown latency | Existing close tests + explicit duration check; add wakeup-on-CLOSING only if shown needed |
| ConsumerManager↔BrokerPollSystem wiring circularity | Setter injection from BrokerPollSystem's constructor; supplier defaults to `() -> false` until wired so standalone ConsumerManager tests keep working |
| Fix regresses drain-time commit behaviour | Full unit suite incl. commit-mode tests; commitSync loop keeps first-try guarantee |
| Chaos test won't run locally (perf-tagged, resource-heavy) | Verified tag/exclusion mechanism; 12-core + Docker matches #29's own dev runs; fall back to fewer instances param variant if the box can't sustain it |
| 5 runs/arm too few for 10-20% signal | Report states sample-size caveat; extend runs if result ambiguous (0/5 vs 1/5 is not decisive — 10 runs if needed) |
| Uber merges pull in #29's test changes that fail on new base | Arms only need compile + the two experiment workloads green-ish; unrelated #29 test failures recorded, not fixed |

## Verification (end-to-end)

1. Commit A: new characterization test green on defective code; full unit suite green.
2. Commit B: test flip RED on old code → GREEN with fix; full unit suite green; one fork=16 run shows the
   10kHz DRAINING spin signature gone.
3. Units 3-4: both arms built; 5+3 runs per arm executed; results table in the report answers the
   prediction; PR #80 updated with all commits.
