# Load-tightness flake family (undiagnosed)

<!-- inflight-type: bug -->
<!-- inflight-impact: blind-spot -->


Shared signature: a **fast-failing** assertion or timeout under heavy contention, passing in isolation
or on rerun. Roster and rates from the 20-run fork16 acceptance hunt on astubbs#80's branch (2026-07-30);
baseline for comparison is 15/20 runs fully clean, zero stall-class failures.

| Test | Rate | Symptom |
|------|------|---------|
| `MultiInstanceMetricsTest.sameRegistryCanBeReusedAfterPcInstanceClosed` | 0/20 hunt, ~1/104 on CI | 1-2s produce/commit lock timeouts |
| `LoadTest` | 1/20 | 60s throughput awaits |
| `DbTest` | 2/20 | postgres container start under contention |
| `KafkaSanityTests`, `TransactionMarkersTest` | singles | residual, uncategorised |
| `PartitionStateCommittedOffsetIT.committedOffsetRemoved[3] none` | 1 sighting (2026-08-05) | `RebalanceInProgressException` out of the test's own setup |
| `TransactionTimeoutsTest.commitTimeout[2]` | 1 sighting (2026-08-06, astubbs#204) | incompletes `[8]` where the parameter pins `[8, 12]` |

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

## A fourth member has left the family: `produceTimeout` is SOLVED (2026-08-13)

It was neither tight nor unforceable. Its phase-2 at-most assertion waited a flat 5s while the commit
block it was checking *also* lasted 5s, started on PC's own commit cadence, and was never tied to it -
so the whole margin was `commit tick - assert poll latency`, **measured at ~500ms**. Fixed by anchoring
the check to the start of the block. **The mechanism, the control arm and the refuted attempts live in
[`docs/solutions/test-flakiness/at-most-assertion-raced-the-block-it-checked-2026-08-13.md`](../solutions/test-flakiness/at-most-assertion-raced-the-block-it-checked-2026-08-13.md)**
- do not restate them here, or the two copies will drift.

Three corrections this leaves behind, because each of them sent an earlier look the wrong way:

1. **The suspect recorded here was wrong.** A lower-base commit splitting offsets 5 and 6 was never
   observed - zero such commits across 9 instrumented runs, and in baseline runs those two records
   completed in the *same millisecond*. That hole is real but unobserved, and is still open by
   construction; it is noted at the trigger site in the test.
2. **"Not reproduced" was a property of the reproducer.** A single-test CPU soak cannot find this:
   burners dilate the commit tick and the poll latency together, leaving their difference intact. The
   margin held at 504-522ms under `SOAK_FREE_CORES=1`. That is why 0/20, 0/3 and 0/45 all came back
   clean while the flake was still live.
3. **A 0-failure soak still told you nothing.** As its own closing line says. What settled this was a
   control arm, not a repetition count.

**What this means for the members still listed above:** before filing any
of them as a tight assertion, check whether the thing being awaited can be *triggered at all* in every
interleaving. A test that waits on a consequence it cannot force is not tight - it is unsound, and
raising its timeout will never fix it. And before trusting a clean soak, check that the reproducer can
move the term you believe is responsible - `produceTimeout` is now the worked example of one that
could not.

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

## `commitTimeout[2]`, for whoever picks it up (seen 2026-08-06 on astubbs#204)

**A different parameter and a different mechanism from the `commitTimeout[1]` entry above**, which left
the family by reclassification (an unforceable trigger). This one is about the assertion, not the
trigger, so neither entry supersedes the other.

Failed with incompletes `[8]` where the `multiple=50` parameter pins `[8, 12]`. The test's own javadoc
names **both** outcomes as physically possible - "just the failed offset (for case where processing
finishes during shutdown timeout)" versus "both offsetToError and offsetToGoVerySlow ... when sleep is
longer than the shutdown timeout" - and the parameterisation pins one of them. So the assertion
encodes a *timing* outcome as if it were deterministic, which is the family signature: correct under
quiet conditions, arbitrary under contention. Ambient probe agreed unprompted: *"probe clean - no
rebalance dwell, no lag stagnation, no frozen partitions observed: the fault is likely in the test
itself, not consumer-group progress."*

**Ruled out as a regression from astubbs#204's `ConsumerManager.commitSync` retry-budget change**, on
four independent grounds, recorded because that PR touches commit-timeout behaviour and the name
collision invites exactly the wrong conclusion:

1. **Wrong code path.** This test runs `CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER`, and
   `AbstractParallelEoSStreamProcessor` selects `committer = producerManager` for transactional mode.
   `ConsumerManager.commitSync` has exactly one caller in main - `consumerMgr.commitSync(offsetsToSend)`
   in `ConsumerOffsetCommitter` - which is only reached in the consumer-sync modes. The changed method
   is never invoked here.
2. **Wrong direction.** That change makes `commitSync` give up *earlier*. Earlier shutdown leaves
   *more* work incomplete, i.e. pushes toward `[8, 12]`. The observed failure is `[8]` - fewer
   incompletes - which is the opposite of what the change could produce even if it were on the path.
3. **Passes locally with the change in place**: `TransactionTimeoutsTest` 3 tests, 0 failures.
4. **Pre-existing membership.** This class is already named as load-sensitive in
   `pc-silent-stall-under-contention-2026-07-29.md` and
   `parallel-integration-tests-flaky-under-concurrency-2026-07-28.md`, and its sibling
   `produceTimeout` is already in the table above.

Do not "fix" this by widening the expected set to accept both outcomes - that would make the test
vacuous, since `[8]` and `[8, 12]` are the only two possibilities. If it is to be deterministic, the
shutdown timeout and the sleep have to be separated far enough that only one outcome is reachable;
otherwise it belongs in the quarantine lane rather than the gating suite.
