# Load-tightness flake family (undiagnosed)

<!-- inflight-type: bug -->
<!-- inflight-labels: concurrency -->
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
| `ParallelEoSStreamProcessorTest.inFlightMessagesCommittedIfProcessedDuringShutdown[1]` | 1/15 (2026-08-07) | `assertCommits(of(1))`, "1 record completed during shutdown", in the transactional arm |
| `PartitionStateCommittedOffsetIT.committedOffsetRemoved[2] earliest` | 1 sighting (2026-08-25, astubbs#353, [job 97859037375](https://github.com/astubbs/parallel-consumer/actions/runs/32865269364/job/97859037375)) | `checkHowManyRecordsWithKeyPresent` expected 2 got 1 - the `[1] latest` assertion signature (solved 2026-08-05 as a nudge race) appearing on the `earliest` parameter; `probe clean` autopsy (test-side, not consumer-group progress), on a branch with no Java <!-- post-merge: checked --> |
| `PartitionStateCommittedOffsetIT.committedOffsetRemoved[1] latest` | 4 sightings (2026-09-01, astubbs#407, [job 100057065090](https://github.com/astubbs/parallel-consumer/actions/runs/33568429332/job/100057065090); 2026-09-01, astubbs#207, [job 99717308477](https://github.com/astubbs/parallel-consumer/actions/runs/33462670308/job/99717308477); 2026-09-02, astubbs#409, [job 100174428838](https://github.com/astubbs/parallel-consumer/actions/runs/33607276956/job/100174428838); 2026-09-02, astubbs#416, [job 100257067886](https://github.com/astubbs/parallel-consumer/actions/runs/33632566980/job/100257067886)) | `checkHowManyRecordsWithKeyPresent` expected 2 got 1, `probe clean`, `forkCount=4` on all four. astubbs#407 captured WHICH record survived - the compactor, not the original - which rules out the 2026-08-05 mechanism; astubbs#409 attached a RATE; astubbs#207 and astubbs#416 captured neither, and astubbs#207's log has expired. Both sections below <!-- post-merge: checked --> |
| `TransactionTimeoutsTest.commitTimeout[2]` | 1 sighting (2026-08-06, astubbs#204) | incompletes `[8]` where the parameter pins `[8, 12]` |

**`committedOffsetRemoved[1] latest` has its own section below**, which owns what the two 2026-09-01 sightings
mean; do not re-derive it here. One correction belongs at the table, though, because it is about how a row
gets read rather than about the defect:

<!-- post-merge: checked-begin -->
**Matching a signature is not classifying a failure, and this row is the worked example.** The astubbs#207
sighting was written up here as a *recurrence* of the mechanism
[`latest-reset-nudge-race-committedoffsetremoved-2026-07-30.md`](../solutions/test-flakiness/latest-reset-nudge-race-committedoffsetremoved-2026-07-30.md)
records as SOLVED - same test, same parameter, same `expected 2 got 1`, same clean probe. The section below
then established from the astubbs#407 sighting that the surviving record is the one the solved mechanism
cannot leave behind, so the signature was never evidence of a recurrence. That reading is withdrawn.

**The recovery was attempted and does not reach it.** `node bin/inflight.mjs codecov test
committedOffsetRemoved` arrived with astubbs#400 and records per-commit outcomes that outlive a CI log - but
its history for this test covers only the branch astubbs#409 ran on, so the astubbs#207 sighting stays
unclassifiable. Recorded so the next reader does not repeat the query hoping for a different answer.

Two things survive it. The astubbs#207 branch is still ruled out **on mechanism**: it changes offset
*decoding*, and this test removes the committed offset entirely, so there is no metadata for its paths to
reach - and the class ran green 7/7 locally at that commit when scoped, red only on CI at `forkCount=4`.
And that sighting is now unclassifiable: **which record survived was never captured, and its log has since
expired**, which is this ledger's own rule about evidence arriving too late, paid in full.

Separately, and still open: the solved doc asserts "ONLY the `[1]=latest` parameter ever fails", which the
2026-08-25 `[2] earliest` sighting in the table above contradicts. Whoever reopens that doc owns the claim.
<!-- post-merge: checked-end -->

**On `inFlightMessagesCommittedIfProcessedDuringShutdown[1]` - read the parameter index before
deciding it is unrelated.** `[1]` is
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
<!-- post-merge: checked -->
deadlock was still on master when this was recorded, with its fix carried by astubbs#29.

## `committedOffsetRemoved[1] latest` - the same assertion as the `[2]` row, failing from the other side (2026-09-01)

<!-- post-merge: checked-begin - names astubbs#407 in the past tense as the branch the sighting came
     from, which stays true once that work has landed -->

`Integration Tests` on astubbs#407's head `7517bd983`
([job 100057065090](https://github.com/astubbs/parallel-consumer/actions/runs/33568429332/job/100057065090)),
`forkCount=4`, one failure in 161 integration tests. Recorded rather than diagnosed, per this
ledger's own rule: the evidence expires with the logs.

**WHICH record survived is the whole point, and it is the reverse of the solved case.** The assertion
is the `checkHowManyRecordsWithKeyPresent("key-" + offset, 2)` inside `causeCommittedOffsetToBeRemoved`,
and the scan came back holding exactly one:

    offset = 202, key = key-50, value = compactor

The 2026-08-05 diagnosis - the nudge race, and the scan-window half now recorded in that method's own
javadoc - was that **the reader stopped early**: extra nudge records pushed the compaction records past
a caller-supplied window, so the scan found the ORIGINAL `value-50` and reported the compactor missing.
Here the scan plainly reached offset 202, because it is holding the compactor; what is absent is the
original. A window that stops short cannot produce that, so **this is not the solved bug wearing its
signature again**, and reading it as one would send the next person to a fix that is already in the
tree. The reading the evidence supports is that the earlier `key-50` was compacted away before the
scan ran - an ordering the test does not control - but that is a hypothesis from one sighting, not a
diagnosis, and it has not been reproduced.

**Master state, not astubbs#407's.** That branch changes one workflow and two Node scripts, and no
Java at all, so nothing in it is reachable from the code under test.

**Do not merge it into the `[2] earliest` row above.** That row carries the same assertion text and the
same `probe clean` autopsy, but on the other parameter, and whether the two share a cause is precisely
what is open. Two sightings on two parameters is not yet a rate on either.

<!-- post-merge: checked-end -->

## `committedOffsetRemoved[1] latest` again, and the first sighting with a RATE attached (2026-09-02)

<!-- post-merge: checked-begin - names astubbs#409 in the past tense as the branch the sighting came
     from; the claim survives that branch merging -->
Same parameter, same assertion as the 2026-09-01 row: `checkHowManyRecordsWithKeyPresent` expected 2,
got 1. Seen on astubbs/parallel-consumer#409, a branch carrying no Java changes at all.

**What is new is not the sighting, it is that a rate came with it.** Every earlier row here records
one job, because a CI log is all anyone had and it expires. This test's per-commit outcome is now
readable from Codecov, and it says:

    inflight codecov test committedOffsetRemoved

    [1]  ! failure  27.5s  770d6f4     <- this sighting
         pass  x6, 47-59s, six earlier commits on the same branch
    [2]  pass x7

**One failure in seven runs on one branch, and the commit it failed at deletes a `.bak` file and
changes nothing else.** A pure file deletion cannot alter a Kafka integration test, so the run-to-run
position of the failure is the evidence, not the commit's content. That is the shape the note above
has been asking for on this family: a rate rather than a verdict from one log.

It does not settle the mechanism. `[1]`'s 2026-09-01 sighting recorded that the record which SURVIVED
was the compactor rather than the original, which the 2026-08-05 nudge-race mechanism cannot produce;
nothing here contradicts or confirms that. What it does settle is that `[1]` is not deterministic on
this branch, which one job could never show.
<!-- post-merge: checked-end -->

## `committedOffsetRemoved[1] latest` a third time, and the rate now spans branches (2026-09-02)

<!-- post-merge: checked-begin - names astubbs#416 in the past tense as the branch the sighting came
     from; the claim survives that branch merging -->
`Integration Tests` on astubbs#416's head `1f844c5fb`
([job 100257067886](https://github.com/astubbs/parallel-consumer/actions/runs/33632566980/job/100257067886)),
`forkCount=4`, one failure in 201 integration tests, `probe clean`. Same assertion and the same
survivor as the 2026-09-01 row: the scan held `offset = 202, key = key-50, value = compactor` and the
original was gone, so this is that shape again and not the solved nudge race. It failed at 25.7s
where every pass in the window below took 46-60s - the fast-failing signature this family is named for.

**The rate is no longer one branch's.** `inflight codecov test committedOffsetRemoved`, read when this
was recorded, showed 13 runs of `[1]` across eight branches in the 35 minutes around this sighting:
this one failure and 12 passes, the next head on the same branch (`0ffab86`) among them; `[2]` and
`[3]` 13 of 13 in the same window. The row above had one failure in seven on one branch; this is the
same failure on a second branch that shares nothing with it - a shell hook, two Node scripts and
docs, no Java. Master state, not astubbs#416's. Still not a mechanism.
<!-- post-merge: checked-end -->

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

### The same defect from the other side: `commitTimeout[1]` produced `[2]`'s outcome (2026-08-25)
<!-- post-merge: checked-begin -->

`Integration Tests` on astubbs#348's head `58d6d38ce`
([job 97653156995](https://github.com/astubbs/parallel-consumer/actions/runs/32797974288/job/97653156995)),
`forkCount=4`. The **`[1]`** arm (`multiple=2`) failed at the committed-offset assertion:

    expected to contain: 12   but was: [8]

**This is the strongest evidence yet for the section above, because it is the mirror image of it.**
The `[2]` sighting had the `multiple=50` arm - the one that pins `[8, 12]`, i.e. "the sleep outlasted
the shutdown timeout" - produce the *other* javadoc'd outcome, `[8]`. This sighting has the
`multiple=2` arm - the one that pins offset 12, i.e. "processing finished during the shutdown
timeout" - produce `[8]` as well, which is `[2]`'s pinned outcome. **Both parameters have now been
observed producing the outcome the other one pins.** Neither arm's expectation is a property of its
parameter; both are properties of a race the test does not control. `[2]` passed in this very run, so
the two arms are not even consistently wrong together.

Ambient probe agreed unprompted, in the same words as the earlier sighting: *"probe clean - no
rebalance dwell, no lag stagnation, no frozen partitions observed: the fault is likely in the test
itself, not consumer-group progress."*

**This does NOT reopen the `commitTimeout[1]` reclassification above.** That entry left the family
because its *trigger* was unforceable - a 35s await on `isClosedOrFailed` that could never fire in
some interleavings, fixed test-side and written up in
[`unforceable-trigger-commit-lock-timeout-2026-08-07.md`](../solutions/test-flakiness/unforceable-trigger-commit-lock-timeout-2026-08-07.md).
That await passed here; what failed is the *post-shutdown assertion*, three statements later. Same
test method, different mechanism, and the reclassification stands.

**Master state, not astubbs#348's.** That head's delta from the one that passed this same suite was
two markdown files, and the `jcstress-poc` module it added has no `<parent>` and no root `<modules>`
entry, so no reactor build compiles it. Nothing in the change was reachable from the code under test.

**Flaky, not deterministic - and that is a measurement, not an assumption.** The next head of the
same branch (`02233811c`, a one-file markdown delta) ran the same suite and `commitTimeout[1]`
passed. So the arm is not simply wrong on this runner; it is wrong when the race falls the other
way, which is what the family signature says. Still worth noting how thin the evidence for
"non-deterministic" is on a single retry: it separates *always red* from *not always red*, and
nothing more.
<!-- post-merge: checked-end -->
