# The confluentinc#909 reproduction rules out the heal paths, but never observes the collision

<!-- inflight-type: task -->
<!-- inflight-impact: test-debt -->
<!-- inflight-state: deferred - needs a broker-verified run to confirm the collision count -->

`RegistrationRaceStaleResidentIT` (`parallel-consumer-core/src/test-integration/.../RegistrationRaceStaleResidentIT.java`)
proves the confluentinc#909 defect by its *consequence* - a produced record never arrives - and
fails a healed run as invalid rather than passing it. What it never does is watch the collision
happen. The guard is therefore indirect, and one specific future change makes it vacuous.

## Why it can go silently green later

The uncovered heal path is `ProcessingShard`'s take-scan stale eviction: `getWorkIfAvailable`
evicts stale containers as it scans, so ordinary churn heals the collision before the fresh
arrival meets it. It is suppressed today only by `WorkManager.getWorkIfAvailable`'s
`requestedMaxWorkToRetrieve < 1` early return, which never lets the scan run while the pipeline is
saturated. **If that arithmetic shifts, the IT passes forever with the defect branch unexercised** -
a reproduction that silently stopped reproducing, which is worse than not having one.

<!-- post-merge: checked-begin -->
The review pass on astubbs#322 narrowed this: the saturation gate and a run-invalid guard now
assert `getNumberRecordsOutForProcessing() == MESSAGE_BUFFER_SIZE` - the actual term in the delta
arithmetic - so a load-factor change fails the run loudly. **The residue is an unknown heal path
that evicts residents WITHOUT de-saturating the pipeline.** Only counting the collisions closes it.
<!-- post-merge: checked-end -->

## The fix, and why it needs a broker rather than a decision

Count stale-resident collisions at insert in `PausableInsertShardManager` and assert the count is
exactly the paused batch's tail - every offset from the pause point to the end of the produced range,
which the test already knows as `RECORD_COUNT - PAUSE_AT_OFFSET`. Write the assertion against those
constants, never against the number they currently multiply out to: the test derives it, and a
number copied here goes stale the moment either constant is retuned while still reading as the
agreed acceptance value. Then the test asserts the mechanism instead of the absence of two known
escapes.

**No production change is needed**, which is the non-obvious part. The instrument already sits in
`bz.stub.parallelconsumer.state`, so `computeShardKey(ConsumerRecord)` and `getShard(ShardKey)` are
both reachable, and `ProcessingShard.getCountOfWorkTracked()` is public. A stale replacement puts
into `entries` without growing it, whereas a fresh add grows it - so a fresh-epoch insert that
leaves `getCountOfWorkTracked()` unchanged landed on an existing resident. In this scenario that
resident is necessarily stale: it comes from the paused batch, whose epoch is older by
construction, so the "exists and is NOT stale, drop the record" branch is unreachable here.

**The cost is the broker run, not the code.** The code is ~10 lines plus one assertion; confirming
the observed count really equals that expression needs Docker and a real broker, and a count that
comes back as anything else is a diagnosis session, not an edit. That is the whole reason it was not applied on astubbs#322. <!-- post-merge: checked -->

## What is already established - do not re-derive it

**The test is falsifiable today**, verified by negative control locally on 2026-08-20, twice -
before and after the review-pass guard strengthening. With `ProcessingShard.addWorkContainer`'s
stale-replacement branch disabled (a direct in-place edit, reverted after), the IT fails on the
stated confluentinc#909-signature assertion - not on a run-invalid guard - after the 90s invariant
await. The green arm passes in ~14.5s. So this note is insurance against future drift, not a
present defect: nothing here says the reproduction is currently unsound.

<!-- post-merge: checked-begin -->
Found independently by two reviewers on astubbs#322 and validator-confirmed; it was that PR's only
P1. Every astubbs#322 reference in this note is past tense on purpose: they name the merged change
this note came out of, and the note itself outlives it - it is deferred on a broker run, not on
astubbs#322.
<!-- post-merge: checked-end --> Related: [`bug-857-family.md`](bug-857-family.md) is a different failure - do not conflate a
909 stale-container collision with an 857 Class 2 stall.
