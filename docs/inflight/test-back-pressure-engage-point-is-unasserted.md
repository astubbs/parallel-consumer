# Nothing asserts WHERE offset-encoding back pressure engages

<!-- inflight-type: task -->
<!-- inflight-impact: test-debt -->

<!-- post-merge: checked-begin -->
Found while reviewing astubbs#351, which un-quarantined
`OffsetEncodingBackPressureTest.backPressureShouldPreventTooManyMessagesBeingQueuedForProcessing`.
Both references to that PR are deliberately past-tense: they name the merged change this note came
out of, so they read correctly once it has landed.
<!-- post-merge: checked-end -->
Two reviewers independently proposed tightening that test's bound on the settled succeeded frontier
so a premature block would go red. **Both proposals were measured and both are refuted** - the gap is
real, but it cannot be closed from inside this test.

**Why the bound cannot be tightened usefully.** The mock consumer hands the whole extra batch over in
one poll, so `offsetHighestSucceeded` jumps 99 -> 139 before the first encode that follows the batch.
That encode therefore always sees the same payload, whatever the threshold is - and the settled
frontier is decided by the *claim boundary*, not by where back pressure engages. Measured by mutating the
guard locally (never committed), the method `docs/testing.md` calls "mutate a guard and see who
notices": changing `updateBlockFromEncodingResult`'s
comparison from `metaPayloadLength > getPressureThresholdValue()` to `... - 2` leaves the test
**green**, logging `Payload size 32 higher than threshold 30.0` in both the mutant and the control.
Mutating it to `... - 10` is killed, but by the *pre-existing* `isAllowedMoreRecords()` assertion
above the section (the partition then blocks during the priming phase), not by anything the frontier
bound could say.

So a regression that engages back pressure moderately early is invisible here, and

- a numeric floor (`isAtLeast(136)`) does not kill the mutant either - 139 >= 136;
- asserting the committed payload crossed the pressure threshold does not kill it either - the payload
  is 32 in the mutant and in the control.

<!-- post-merge: checked -->
**Where the coverage belongs.** astubbs#351 built a deterministic single-threaded probe - prime 100,
hold `{0, 2}`, add 40, `forcedCodec = BitSetV2`, `DefaultMaxMetadataSize = 40`, taking and succeeding
one record at a time - which reports the block point exactly (136 for that configuration) because it
controls the claim boundary. That probe was used for diagnosis and not landed. Landing it as a unit
test is what makes a premature-engage regression red; see
[`docs/solutions/test-flakiness/back-pressure-freezes-the-frontier-the-test-asserted-2026-08-24.md`](../solutions/test-flakiness/back-pressure-freezes-the-frontier-the-test-asserted-2026-08-24.md),
section "Measured, not inferred", for the table it produces.

**Do not fold this into the existing test.** `OffsetEncodingBackPressureUnitTest` cannot host it
either: it succeeds every extra record synchronously before encoding, so it never observes a frozen
frontier at all. This wants its own deterministic test with no threads.

Related, and deliberately left open with it: nothing pins that `getOffsetHighestSucceeded()` and
`getOffsetHighestSeen()` diverge while a partition is blocked, so a regression collapsing the two
accessors is invisible to the round-trip assertion in `OffsetEncodingBackPressureTest`. The same
probe is the sound home for that too - an inline check would only fire in the minority of runs where
extras are left untaken.
