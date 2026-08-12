# Load-tightness flake family (undiagnosed)

Shared signature: a **fast-failing** assertion or timeout under heavy contention, passing in isolation
or on rerun. Roster and rates from the 20-run fork16 acceptance hunt on astubbs#80's branch (2026-07-30);
baseline for comparison is 15/20 runs fully clean, zero stall-class failures.

| Test | Rate | Symptom |
|------|------|---------|
| `MultiInstanceMetricsTest.sameRegistryCanBeReusedAfterPcInstanceClosed` | 0/20 hunt, ~1/104 on CI | 1-2s produce/commit lock timeouts |
| `TransactionTimeoutsTest.produceTimeout` | 1/20 + 1 highcpu (2026-07-30); **0 in all three reproducers 2026-08-07** - see below | assertion failure inside the produce-timeout test; which assertion is the open question - see below |
| `LoadTest` | 1/20 | 60s throughput awaits |
| `DbTest` | 2/20 | postgres container start under contention |
| `KafkaSanityTests`, `TransactionMarkersTest` | singles | residual, uncategorised |
| `PartitionStateCommittedOffsetIT.committedOffsetRemoved[3] none` | 1 sighting (2026-08-05) | `RebalanceInProgressException` out of the test's own setup |
| `ParallelEoSStreamProcessorTest.inFlightMessagesCommittedIfProcessedDuringShutdown[1]` | 1/15 (2026-08-07) | `assertCommits(of(1))`, "1 record completed during shutdown", in the transactional arm |

**On that last one - read the parameter index before deciding it is unrelated.** `[1]` is
`PERIODIC_TRANSACTIONAL_PRODUCER`, not the consumer-commit arm, so it lands on whatever transactional
change is in flight and looks like a regression. It was seen once, in a full-suite run on
astubbs/parallel-consumer's produce-lock double-release branch, and did not reproduce: 6/6 in
isolation, 1 failure in 15 runs on that branch overall, and 0/4 on unmodified `master` in a
same-magnitude sequential control. Zero-in-four cannot rule out a ~7% flake, so the control shows only
that there is no *elevated* rate - it is not a clean bill of health for `master`, and nobody should
cite it as one. The branch was cleared on mechanism instead: the test uses `poll()`, which never
reaches `processAndProduceResults`, so no produce lock is ever set on its contexts and both the old and
new release paths are no-ops for it.

**A third member has now left the family, and it left by being reclassified rather than fixed-as-tight.**
`TransactionTimeoutsTest.commitTimeout[1]` failed once on CI (2026-08-07,
[job 92733771394](https://github.com/astubbs/parallel-consumer/actions/runs/31135433520/job/92733771394?pr=219);
the run reports success because attempt 2 passed on the identical tree). It reads exactly like this
family - an await that expired under load - and it is **not** a member. The 35s await is not the
margin; it is the deadline for a *consequence*. The margin belongs to the **trigger**, and the trigger
is a `TimeoutException` from `ProducerManager.acquireCommitLock` that only occurs if the controller
attempts a commit *while* the slow record holds the produce lock. Two things stop that, neither of them
tightness:

1. too little margin between the record's sleep and `commitLockAcquisitionTimeout`;
2. `wm.isDirty()` - AND-ed into the commit gate, single setter `PartitionState#onSuccess` - suppressing
   the commit attempt entirely, so there is no deadline to widen.

Fixed test-side by making the overlap deterministic. **The mechanism, the ruled-out readings and the
experiment numbers live in
[`docs/solutions/test-flakiness/unforceable-trigger-commit-lock-timeout-2026-08-07.md`](../solutions/test-flakiness/unforceable-trigger-commit-lock-timeout-2026-08-07.md)**
- do not restate them here, or the two copies will drift.

## `produceTimeout`: investigated 2026-08-07, not reproduced, and the old label was wrong

Do **not** start from "tight assertion", and do not start from the trigger. Both were checked.

**Its trigger is properly latched** - the injected `sendOffsetsToTransaction` counts the latch down
*while already holding the commit write lock* and then sleeps, and the worker awaits that latch before
sleeping again and attempting the produce lock against a shorter timeout. Real margin, forced
ordering. So `produceTimeout` is **not** the unforceable-trigger class, whatever its sibling turned out
to be.

**The suspect, if it flakes again, is phase 2's `assertConsumedAtMostOffset`.** That helper waits, then
checks **once**, and it needs *no* transaction to commit new output records in the whole window. The
injected sleep only fires when the commit's base offset is exactly `OFFSET_TO_PRODUCE_SLOWLY` - which
requires the two records below it complete and it not (`PartitionState#getOffsetToCommit` is "one below
the highest sequentially succeeded offset"). A commit tick landing after the first completes but before
the second does has a lower base, injects no sleep, commits for real, and the at-most assertion loses.
Nothing in the test prevents that interleaving.

**Not reproduced, at these N** - report the rate, not a verdict:

| Reproducer | Result |
|---|---|
| single test + CPU burners, `SOAK_FREE_CORES=1` | 0/20 |
| full forked IT suite, `rerunFailingTestsCount=0` | 0/3 |
| CI surefire flake markers, 45 runs | 0 sightings |

The mechanism is unchanged since the 1/20 was measured - no main-code commit has touched
`ProducerManager`, `PartitionState` or `AbstractParallelEoSStreamProcessor` since - so this is **not**
"fixed by something else". Three suite runs is a small N against 1-in-20; treat it as "not flaking at a
detectable rate today", nothing stronger.

**Use the right reproducer.** A single-test CPU soak is the wrong shape for this one: the interleaving
above needs the gap between two records completing to stretch, which is broker latency, not CPU. The
original rate came from the whole suite forked per core. Disable `rerunFailingTestsCount` when hunting,
or CI's own retry will hide the failure you are trying to catch (see
[`../solutions/workflow-issues/ci-retries-hid-flakes-from-the-ledger-2026-08-07.md`](../solutions/workflow-issues/ci-retries-hid-flakes-from-the-ledger-2026-08-07.md)).

**What this means for the members still listed above:** before filing any
of them as a tight assertion, check whether the thing being awaited can be *triggered at all* in every
interleaving. A test that waits on a consequence it cannot force is not tight - it is unsound, and
raising its timeout will never fix it. (`produceTimeout` already latches its own trigger, so it is the
worked example of doing this right.)

**Classify before touching any of them** (the astubbs#68 lesson): this family is exactly where the upstream
confluentinc#857 deadlock and the drain zombie were hiding, and both looked like tightness first. Two members have
since been *solved* and left the family, which gives you their signatures to rule out - the nudge race
is an unwinnable await plus a `SubscriptionState` reset positioned past the data
(`latest-reset-nudge-race-committedoffsetremoved-2026-07-30.md`), and the drain zombie is a poll spin
in `DRAINING` state (`pc-silent-stall-under-contention-2026-07-29.md`).

The `committedOffsetRemoved[3] none` sighting, for whoever picks it up: only the `NONE` parameter runs
that setup block, where a `subscribe`d raw `KafkaConsumer` gets a single `poll(1s)` to complete its join
before `commitSync` - not a guarantee under contention. Seen once, in a deliberately CPU-loaded probe
run during astubbs#115, and one sighting is not a rate. The exception names its own remedy, which makes
"just await assignment" tempting; resist that until it is classified, because astubbs#100 fixed a *main-code*
bug in exactly this area (a rebalance-time commit killing the broker-poll thread). Reproduce with
`bin/soak-test.sh 'PartitionStateCommittedOffsetIT#committedOffsetRemoved' 20` at a low
`SOAK_FREE_CORES`.

**Explicitly NOT a member: `RebalanceEoSDeadlockTest.noDeadlockOnRevoke`** (1/20). Per the astubbs#68 record
its contended failure maps to the real confluentinc#857 deadlock - that sighting is live confirmation the
deadlock is still on master, with its fix waiting in astubbs#29.
