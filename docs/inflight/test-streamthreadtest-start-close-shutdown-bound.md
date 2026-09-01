# `StreamThreadTest`'s start/close case can miss its 10-second shutdown bound on CI

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->
<!-- flaky-case: org.apache.kafka.streams.processor.internals.StreamThreadTest#shouldChangeStateAtStartClose -->

`StreamThreadTest.shouldChangeStateAtStartClose[1]` failed in the **seam-off** arm of the seam-on
evidence lane on a GitHub-hosted runner, on:

```
Condition not met within timeout 10000. Thread never shut down. ==> expected: <true> but was: <false>
```

**One sighting.** It is recorded rather than re-run because a control-arm failure is what the lane
refuses to difference against, so it has to be either diagnosed or admitted - and admitting it
without writing down why would be the silence this ledger exists to replace.

## The argument that it is a timing bound and not the patch

**The same case PASSED in the seam-ON arm of the same run.** That is the whole argument and it is
close to a control arm handed over for free: the two arms ran the same patched classes minutes apart
on the same runner, differing only in the dispatch switch. A defect the patch introduced cannot be
*repaired* by turning the seam on - the seam-on path is strictly more machinery on top of the same
patched code, not less of it. What varies with load, and only with load, is whether a thread finishes
shutting down inside a fixed ten seconds.

Supporting, weaker: the seam-off oracle runs at zero failures repeatedly on a developer machine, and
the assertion is an Awaitility deadline on Kafka's own shutdown rather than an assertion about a
value. It is Kafka's test, on Kafka's code, and the patch touches neither side of that wait with the
seam off - which is the module's central behaviour-preservation claim.

## What would settle it, and what it is NOT

**A second sighting, with its arm.** The lane runs on every push and every merge to master, so the
rate accumulates on its own. If it ever fails in BOTH arms of one run, the asymmetry argument above is
void and this note is wrong.

**It is not a quarantine candidate.** Quarantine is master-state, this is one sighting on one PR, and
the case lives inside Apache Kafka's own compiled test class where the annotation has nowhere to go -
the same obstacle `test-streamthreadtest-invalid-timestamps-flake.md` records for its own case.

**The marker is not a silencer.** The lane still prints this failure, still names this note beside it,
and still refuses to attribute it to the seam. What the marker buys is that one timing bound on
Kafka's own test no longer withholds the verdict on the divergence set, which would leave the lane
permanently red for a reason that is not a divergence - and a permanently red lane is one nobody
reads.

## What this sighting incidentally proved

The lane's control-arm integrity check had never fired on real data before this run. It did exactly
what it was built to do: named the ledgered flake beside it, refused to produce a divergence verdict
while an undiagnosed control-arm failure stood, and said so in a verdict that outranked the
classification - which was itself clean, with no unexplained divergences.

## Delete when

A second sighting has settled it either way, or the case stops appearing.
