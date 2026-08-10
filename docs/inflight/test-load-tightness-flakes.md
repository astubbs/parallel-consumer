# Load-tightness flake family (undiagnosed)

Shared signature: a **fast-failing** assertion or timeout under heavy contention, passing in isolation
or on rerun. Roster and rates from the 20-run fork16 acceptance hunt on astubbs#80's branch (2026-07-30);
baseline for comparison is 15/20 runs fully clean, zero stall-class failures.

| Test | Rate | Symptom |
|------|------|---------|
| `MultiInstanceMetricsTest.sameRegistryCanBeReusedAfterPcInstanceClosed` | 0/20 hunt, ~1/104 on CI | 1-2s produce/commit lock timeouts |
| `TransactionTimeoutsTest.produceTimeout` | 1/20 + 1 highcpu | tight produce-timeout assertion |
| `LoadTest` | 1/20 | 60s throughput awaits |
| `DbTest` | 2/20 | postgres container start under contention |
| `KafkaSanityTests`, `TransactionMarkersTest` | singles | residual, uncategorised |
| `PartitionStateCommittedOffsetIT.committedOffsetRemoved[3] none` | 1 sighting (2026-08-05) | `RebalanceInProgressException` out of the test's own setup |

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

**What this means for the members still listed above, `produceTimeout` especially:** before filing any
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

**`ParallelEoSStreamProcessorTest`'s shutdown family is unstable locally - three sightings, one session.**
On 2026-08-10, six full reactor runs on astubbs#240's branch produced three failures, each a different
test, none reproducing in isolation: `executorThreadsInterruptedOnShutdownTimeout[1]` (detailed below),
`inFlightMessagesCommittedIfProcessedDuringShutdown[3]` failing through
`AbstractParallelEoSStreamProcessorTestBase.assertCommits` with `[1 record completed during shutdown]`,
and `JStreamParallelEoSStreamProcessorTest.testConsumeAndProduce` once. **Not a rate** - six runs under
varying background load is not a controlled sample, and none of them was designed as one.

Two of the three are shutdown-commit siblings, which matters because
[`test-untracked-ci-flakes.md`](test-untracked-ci-flakes.md) already tracks a third member of that same
family - `queuedMessagesNotProcessedOrCommittedIfSubmittedDuringShutdown`, 3/45 on CI - as a **regression
of astubbs#101**, surfacing through that same `assertCommits` helper. Independent sessions hitting
different members of one family is stronger signal than any single sighting, and argues for treating the
shutdown-commit group as one investigation rather than three flakes. Whoever picks it up should start
from the astubbs#101 fix and diff against it, per that entry.

**Candidate, unconfirmed - and it looks like the unforceable-trigger class, not tightness.**
`ParallelEoSStreamProcessorTest.executorThreadsInterruptedOnShutdownTimeout[1]` failed once
(2026-08-10, astubbs#240's branch) during a reactor run with several concurrent Maven builds competing:
`Expecting AtomicBoolean(false) to have value: true`. It then passed **0/6 unloaded and 0/8 at
`SOAK_FREE_CORES=2`**, plus CI and a clean reactor run - not reproduced, so no rate, and one sighting is
not a rate. Apply this doc's own rule before filing it as tight: the test sets a 1s `shutdownTimeout`,
blocks the user function on a latch, closes, and asserts the worker caught `InterruptedException` - but
between priming and closing it waits on `awaitForSomeLoopCycles(2)`, which awaits **control-loop cycles**,
a proxy for the antecedent the assertion needs (*a worker is inside the user function*). If dispatch has
not happened when `close()` fires there is nothing blocked to interrupt and the assertion fails exactly
as seen. That is a trigger the test cannot force, so raising a timeout would never fix it; the fix is to
have the user function count down an `entered` latch and await that instead. Note the sibling in the same
class, `queuedMessagesNotProcessedOrCommittedIfSubmittedDuringShutdown` (3/45), is already tracked in
`test-untracked-ci-flakes.md` as a regression of astubbs#101 - same class, same shutdown-commit area,
so rule the two in or out together rather than separately.

**Explicitly NOT a member: `RebalanceEoSDeadlockTest.noDeadlockOnRevoke`** (1/20). Per the astubbs#68 record
its contended failure maps to the real confluentinc#857 deadlock - that sighting is live confirmation the
deadlock is still on master, with its fix waiting in astubbs#29.
