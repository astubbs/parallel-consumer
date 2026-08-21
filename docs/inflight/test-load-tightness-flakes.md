# Load-tightness flake family (undiagnosed)

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
| `PartitionStateCommittedOffsetIT.committedOffsetRemoved[1] latest` | 1 sighting (2026-08-21) | `checkHowManyRecordsWithKeyPresent` saw 1 record where 2 were expected |

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

### `committedOffsetRemoved[1] latest` - same test and parameter as a SOLVED flake, different assertion (2026-08-21, astubbs#328)

Seen once on astubbs#328's Integration Tests job, which had passed on that PR's previous head.
Not that PR's doing: it changes no `parallel-consumer-core` code, and its one core edit is a javadoc
comment.

```
checkHowManyRecordsWithKeyPresent:538  expected: 2  but was: 1
myCollection was: [ConsumerRecord(topic = LoadTest-1081004001, partition = 0, offset = 202,
                   key = key-50, value = compactor)]
```

**Do not file this under the solved `[1] latest` write-up without checking, and here is why.** That
one - `latest-reset-nudge-race-committedoffsetremoved-2026-07-30.md` - is the same test AND the same
parameter, which makes it the obvious home. Its signature is a `ConditionTimeoutException` on "not to
be empty": **zero** records, an await that never completes. This sighting is a *count* - one record
where two were expected, arriving fast enough to fail the assertion rather than time it out. Same
door, different failure. Assuming they are one thing would either resurrect a closed investigation or
bury a new one.

The remaining record is the compaction simulation's own (`value = compactor`), so what is missing is
the other one the assertion expects to still be present. Whoever picks this up wants the same
reproduction the `[3] none` sighting names below, pointed at the `latest` parameter - and should
treat one sighting as one sighting, not a rate.

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
