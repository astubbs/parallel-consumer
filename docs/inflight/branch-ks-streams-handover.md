# Kafka Streams on PC (astubbs#255) - handover

State as of 2026-08-13. Nine branches, one open PR. Read this before touching any of them.

## Branches

Tips are short SHAs. "ahead" means local commits not on origin.

| Branch | Tip | Remote | What it is |
|---|---|---|---|
| `feats/ks-on-pc-spike` | `abcc811e` | pushed | **The base.** PR astubbs#271. Dispatch seam, crash safety (U9), module README. |
| `feats/ks-streams-wake-on-work` | `e735cdbc` | pushed | Split poll wait. Single-key control 0.69x -> 0.99x. |
| `feats/ks-streams-refuse-unsupported-surface` | `801c8742` | pushed | Refuses joins/windows/suppression/EOS. 460-line README lives here. |
| `feats/ks-streams-task-lifecycle-and-rebalance` | `3770afe2` | pushed | U10. Pile B 4->1. Multi-instance rebalance test. |
| `feats/ks-streams-stream-time-lowwater` | `41df54fb` | pushed | U13. Stream time via in-flight low-water mark. |
| `feats/ks-streams-backpressure-and-error-surfacing` | `9895a1b2` | pushed | U14. `maxBufferedSize` backpressure, error surfacing. |
| `test/ks-streams-realistic-domain-benchmark` | `24928ab8` | pushed | Realistic + synthetic benchmark suite, `DEMO.md`. |
| `feats/ks-streams-pc-example` | `354711c6` | pushed | Runnable demo module, 20-30s. |
| `feats/streams-dispatch-streamsconfig-property` | `0ce15fc3` | pushed | Per-instance `StreamsConfig` dispatch switch. **Unfinished.** |

All nine are on origin at those SHAs, re-derived rather than copied. The six previously marked LOCAL were
pushed after this file was written, and the base moved by one commit - this file's own.

Three branches were added afterwards and are not part of the nine above:

| Branch | Off | What it is |
|---|---|---|
| `feats/ks-streams-punctuator-commit-coverage` | `…stream-time-lowwater` | Evidence that the Kafka Streams commit sensor is blind on the PC path. Four arms. |
| `feats/ks-streams-postcommit-checkpoint-gap` | `…punctuator-commit-coverage` | Refutes "the PC path never checkpoints" with the 12,000-record run that disproves it. |
| `feats/ks-streams-punctuator-effect-survival` | `…postcommit-checkpoint-gap` | Answers defect 1: punctuator forwards and store writes survive a hard abort with no commit in the window. |
| `docs/ks-streams-correct-commit-coverage-claims` | `…punctuator-commit-coverage` | Shrinks the U13 inflight entry to what measurement left of it. |

**Topology.** All descend from the base. Not a linear stack: each was cut from a different point and they were reconciled by merging the base forward, not rebasing. **Always merge, never rebase** - other work builds on these and rebasing forces a force-push.

Every branch carries `8a0762a4` (the cross-thread fix) and everything before it. Only the base's last two doc commits have not propagated.

## PRs

- **astubbs#271** (`feats/ks-on-pc-spike`) - all checks green. **BLOCKED by 10 unresolved review threads**, not by CI. Four are uncovered by in-flight work; *"a worker's failure can be committed past"* is the one to answer first. Others map to U13 (STREAM_TIME punctuation), U14 (backpressure), U10 (`revive()`).
- **astubbs#274 / astubbs#275** - the review-gate fix, split in two. #274 is workflow-only and **its `claude-review` cannot pass by construction** (the action refuses to review a PR editing its own workflow). A human must merge it knowingly; that unblocks #275.

## Traps that have cost real time

- **`-Dtest=` silently skips the upstream Kafka suite.** It overrides that execution's `<includes>`, the suite never runs, and the build goes green having computed nothing. Cost three agents a run. Isolate with `-Dincluded.groups=<nonexistent>` and read counts from `surefire-reports-kafka-upstream/`.
- **`419 run, 0 failures` is NOT deterministic.** `StreamThreadTest.shouldLogAndRecordSkippedRecordsForInvalidTimestamps[3]` is a pre-existing flake, roughly 2 in 5. If that exact case fails, re-run. Anything else is real. See `test-streamthreadtest-invalid-timestamps-flake.md`.
- **Never hand-edit `pc-streams.patch`.** Its `@@` headers encode line counts. Regenerate: `./mvnw -pl .,parallel-consumer-streams process-sources` (NOT `generate-sources`, which only unpacks), edit under `target/kafka-patched/`, then `bin/regen-patch.sh` with **no maven run in between**.
- **The hunk count is a proxy, not proof.** Adding lines can MERGE hunks and lower it legitimately. Verify by content: every line the old patch added must still be added.
- **`-pl parallel-consumer-streams` alone fails** at `enforcer:enforce`. Use `-pl .,parallel-consumer-streams`.
- **Counts are branch-dependent and have drifted repeatedly.** Seam-off upstream total is 188 on the base and 419 above wake-on-work; seam-on `StreamTaskTest` is 67/101 on the base and 65/101 with refusals. **Re-derive, never copy.**

## Open defects, ranked

1. **Whether a punctuator's own effects survive an actual process death, and whether they re-fire over already-covered event time on rebalance.** Both still open. `WALL_CLOCK_TIME` punctuators also fire unwarned where `STREAM_TIME` logs - pre-existing rather than introduced by U13. U10's territory.

   **A previous revision of this entry downgraded this to "a missing warning, not data loss" on the strength of a test that was measuring its own shutdown. Retracted.** `feats/ks-streams-punctuator-effect-survival` read the topics *after* `streams.close()`, and close runs a clean shutdown - `prepareCommit()` -> `flush()` -> `streamsProducer.flush()` -> commit -> `producer.close()`, which blocks until every buffered record is sent. Setting `linger.ms` to five minutes made the point: the old shape still passed, in an eleven-second run. Two of its premises were false as well - `StreamThread.lastCommitMs` starts at zero, so Streams commits on its first run-loop iteration rather than 30s in, and nothing reached the broker "without a flush".

   **What that branch establishes now, with a negative control behind it:** punctuator forwards and store writes reach the broker on the producer's own schedule, before and independently of any commit. **What it does not establish:** anything about process death. `abortAllActive()` is `workerPool.shutdownNow()` and nothing more - the producer, `StreamThread` and punctuator all keep running, and the punctuator runs through `maybePunctuateSystemTime`, which is byte-for-byte stock and never enters PC dispatch. A real answer needs a forked JVM and a SIGKILL.

   **Also measured and does not hold:** *offsets never commit* - they do; and *no checkpoint* - `postCommit` runs under load (12,000 records checkpointed at changelog 11,862-11,929 against stock's 11,999). `TaskExecutor` walks its tasks twice; loop 2 re-asks `commitNeeded()` after `onCommitSuccess` has cleared it, so the *sensor* misses the commit rather than the commit not happening. What survives is an idle-window tail bounded by the final commit round, with clean `close()` checkpointing regardless. The `|| commitNeeded` candidate cannot be evidenced through commit cadence - `commit-total` is pinned at zero on an idle PC task. Evidence on `feats/ks-streams-punctuator-commit-coverage` and `feats/ks-streams-postcommit-checkpoint-gap`; three rejected observables are recorded in `PunctuatorCommitCoverageTest`'s javadoc.

   **Re-ranked down from "their effects never become commit-covered", which measurement did not support.** Offsets commit on the PC path, and `postCommit` runs under load - 12,000 records through a stateful topology checkpointed at changelog 11,862-11,929 against stock's 11,999. `TaskExecutor` walks its tasks twice; loop 1 commits PC's frontier, and loop 2 re-asks `commitNeeded()` after `onCommitSuccess` has cleared it, so the *sensor* misses the commit rather than the commit not happening. What survives is an idle-window tail: no work completing between the commit and loop 2 skips that round's `postCommit`, bounded by the final commit round, with clean `close()` checkpointing regardless. The `|| commitNeeded` candidate also cannot be evidenced through commit cadence - `commit-total` is pinned at zero on an idle PC task. Evidence on `feats/ks-streams-punctuator-commit-coverage` and `feats/ks-streams-postcommit-checkpoint-gap`; the reasoning and three rejected observables are in `PunctuatorCommitCoverageTest`'s javadoc.
2. **U14: `countRecordsPcWillQueue` counts re-delivered offsets that `ProcessingShard` drops** - permanent-pause residue. U14's own "pick this up first".
3. **U14's U4 memory-bound proof was never built.** The headline requirement of that unit. Pile C tests are the check on the fix, not the goal.
4. **U13's three recorded open items** - the seed not reaching `pcRecordQueues` (breaks `UsePartitionTimeOnInvalidTimestamp` after restart), `close()`'s drain advancing over work a forced shutdown killed, and a `RejectedExecutionException` hold leak that pins the mark forever.
5. **The per-instance dispatch test is orphaned.** `feats/streams-dispatch-streamsconfig-property` has the implementation (`PcDispatchSettings`, precedence config > system property > default ON) but not the test proving two `KafkaStreams` instances in one JVM get different settings - which is the whole point, since the old process-global design passes any single-instance test. **Three agents died at that exact point on an API content filter.** Reproducible trigger.

## Decisions already settled - do not relitigate

- **This module does not gate release 0.6.0.0.** Whatever state it is in when the release cuts is what ships. No MVP bar. Merging is cheap to reverse (leaf module); publishing is not - **merge freely, publish deliberately**.
- **Lead with backlog catch-up (3.72x, 47s to 15s), not the 57x.** 57x is bounded by the fixture's own 1500-over-25 construction. The **median** is the speedup; the **minimum** states the claim and is not a multiplier.
- **Every figure ships with its control.** The upstream pass count with the seam-on figure; the head-of-line result with the single-key control and the within-one-partition and blocking-IO caveats.
- **Packaging is parked**, with all options recorded in the plan. Both jars on one classpath is the unsolved problem; the crux is coordinates, not the fork.
- **Annotate and throw, never delete a signature** - Kafka's own suite runs as pre-compiled classes and would link-fail.
- **The README moves in lock step with the code**, in the same unit as the change.

## Where things are written down

- Plan: `docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md` (KTDs are settled; `Current Shortcomings` is the living gap list the README points at)
- Result: `docs/plans/2026-08-08-002-ks-on-pc-spike-result.md`
- Benchmark results: `docs/plans/2026-08-11-001-realistic-benchmark-result.md`, front door `parallel-consumer-streams/DEMO.md`
- Worklist: `docs/inflight/pr-ks-spike-next-work.md`; rebalance gaps: `pr-streams-rebalance-coverage-gaps.md`
- Eight learning docs in `docs/solutions/` from this work. Vocabulary in `CONCEPTS.md`.

## One habit worth keeping

Almost every real defect this session was found by a control arm, not by review-as-reading: the guard that exposed a silent cross-thread race, the ablation that showed wake-on-work was two thirds of the backlog benefit, the mutation that showed nine stream-time tests derived their expectation from the record. **Several tests here passed while proving nothing** - a shutdown proof whose counter was monotonic, a restart assertion satisfiable by pre-crash data, a rebalance assertion mathematically entailed by another. Before trusting a green test, ask what would have to break for it to go red.
