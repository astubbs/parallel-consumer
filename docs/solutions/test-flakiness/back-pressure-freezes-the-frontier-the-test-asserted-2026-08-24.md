---
title: "Back pressure froze the very frontier the test asserted: OffsetEncodingBackPressureTest expected the last polled offset from a mechanism that stops offsets succeeding"
date: 2026-08-24
category: test-flakiness
module: parallel-consumer-core
problem_type: flaky_test
component: testing
symptoms:
  - "OffsetEncodingBackPressureTest.backPressureShouldPreventTooManyMessagesBeingQueuedForProcessing: ConditionTimeoutException, `expected: 139 but was : 136 within 30 seconds`"
  - "Actual differs run to run (136 and 132 both recorded); a 30s wait never rescues it"
  - "4/45 on the unit lane - the most frequent tracked flake, quarantined as an explicit rule-1 exception with no diagnosis"
root_cause: test_design_bug
resolution_type: test_fix_assert_the_settled_frontier_not_the_last_polled_offset
severity: low
status: "SOLVED - test-side fix. PC is healthy; the refusal that freezes the frontier is the back-pressure mechanism working as designed."
last_updated: 2026-08-24
related:
  - "docs/inflight/test-untracked-ci-flakes.md - the ledger this was open on; entry shrunk to point here"
  - "vacuous-await-condition-brokerpoller-backpressure-2026-07-31.md - same area, same method (do the arithmetic: can the condition be reached from the system's caps?), different test"
  - "at-most-assertion-raced-the-block-it-checked-2026-08-13.md - the control-arm method used to settle this"
  - "docs/investigating.md - a fix that works is not evidence of the cause"
---

# The assertion demanded a frontier that back pressure exists to stop advancing

`backPressureShouldPreventTooManyMessagesBeingQueuedForProcessing` primes 100 records (holding
offsets 0 and 2 incomplete), then sends 40 more to push the `BitSetV2` encoding past the size
threshold that blocks the partition. Having awaited the block, it asserted the committed payload's
high-water mark equalled the last offset it sent:

```java
int expectedHighestSeen = numberOfRecordsToPrimeWith + extraRecordsToBlockWithThresholdBlocks - 1; // 139
Truth8.assertThat(incompletes.getHighestSeenOffset()).hasValue(expectedHighestSeen);
```

Three facts make that unreachable except by luck, and all three are readable in the source:

1. **The encoded high-water mark is the highest *succeeded* offset, not the highest polled one** -
   `PartitionState` carries the comment `use offsetHighestSucceeded instead of offsetHighestSeen`,
   and `OffsetMapCodecManager#encodeOffsetsCompressed` takes its range top from
   `getOffsetHighestSucceeded()`.
2. **Back pressure stops offsets succeeding.** It does not pause the poller - that is the separate
   load gate, `wm.shouldThrottle()`. It gates `PartitionState#couldBeTakenAsWork`, which refuses
   every record at or above the highest succeeded offset once `isAllowedMoreRecords()` is false.
3. So the moment the partition blocks, the succeeded frontier **freezes** at the top of the last
   batch already claimed as work. Everything above it is refused, forever, until the record holding
   the partition completes - which this test only releases in a later section.

The expectation therefore holds only when the control loop happens to claim the whole 40-record
extra batch **before** the commit tick that crosses the threshold. That usually happens (the mock
consumer hands over all 40 in one poll), which is why the test mostly passed.

## Measured, not inferred

A deterministic probe replaying the test's exact configuration with no threads at all - prime 100,
hold `{0, 2}`, add 40, `forcedCodec = BitSetV2`, `DefaultMaxMetadataSize = 40` so the pressure
threshold is 30 - taking and succeeding one record at a time:

| highest succeeded | payload chars | blocked |
|---|---|---|
| <= 119 | 28 | no |
| 120-127 | 29 | no |
| 128-135 | 30 | no |
| **136** | **31** | **yes** |

At 136 the next `getWorkIfAvailable` returns nothing: offsets 137, 138 and 139 are refused and the
frontier is frozen at 136 - **the exact actual the CI failure reported**.

The failure was then reproduced in the real test, deterministically, by splitting the extra send
into 37 + 3 with the block confirmed between them:

```
org.awaitility.core.ConditionTimeoutException:
expected: 139
but was : 136 within 30 seconds.
```

Character-for-character the CI signature.

## Control arm

Move the claim boundary and the threshold; the frozen frontier moves with them, and is never the
constant the test asserted:

| `DefaultMaxMetadataSize` | first chunk | settled frontier | last offset sent |
|---|---|---|---|
| 40 | 37 | **136** | 139 |
| 38 | 25 | **124** | 139 |
| 38 | 30 | **129** | 139 |
| 38 | 37 | **136** | 139 |

That is the recorded falsification path's first arm firing: *the actual value tracks the encoding
block point*. The second arm - "the high-water mark reaches 139 given long enough, so this is a
slowness problem" - is dead: the probe reaches the frozen state with no time pressure at all, and
no wait can move it.

## The fix

Test-side, in three parts:

- **The exact constant 139 moves to where it is true.** Back pressure gates which records may be
  *taken*, not which are *seen*, so every extra record is polled and registered even while blocked -
  `wm.getPm().getHighestSeenOffset(topicPartition)` does reach 139, deterministically. The former
  check there was `isGreaterThan(numberOfRecordsToPrimeWith)`, so this is a strengthening, not a
  relaxation: the precision was relocated to the value that can hold it, not discarded.
- **Await quiescence before reading the frontier.** Once blocked, nothing further can complete, so
  in-flight settles at exactly the two records the test is deliberately holding. Awaiting
  `getNumberRecordsOutForProcessing() == numberOfBlockedMessages` makes the subsequent
  `getOffsetHighestSucceeded()` read a still value - obeying the rule from
  [`vacuous-await-condition-brokerpoller-backpressure-2026-07-31.md`](vacuous-await-condition-brokerpoller-backpressure-2026-07-31.md):
  do not compare two moving values; await a quiescent state, then read both.
- **Assert the payload records that settled frontier**, having separately asserted the frontier
  advanced past the primed batch. What the section is actually about - "assert blocked, but can
  still write payload" - is preserved and now non-vacuous.

Verified:

| Arm | Result |
|---|---|
| original assertion + forced split send | **RED**, `expected: 139 but was : 136` (47.6s, full 30s timeout burned) |
| fixed assertion + forced split send | GREEN (10.6s) |
| fixed assertion, stock test | GREEN 5/5 |
| fixed assertion, four control-arm configurations above | GREEN 4/4 |
| whole `offsets` package | GREEN, 144 tests |

## Classification: what this was NOT

- **Not a product bug.** Refusing records at or above the highest succeeded offset is the
  back-pressure mechanism working: taking them would extend the encoded range and grow the payload
  further, which is the thing being resisted.
- **Not the torn-read family.** In `encodeOffsetsCompressed` the range top is
  `getOffsetHighestSucceeded()`, so what the test decodes is highest *succeeded* at encode time, not
  highest polled. astubbs#344's tear makes the encoded range *wider* than the incompletes snapshot,
  pushing that value **up**; this failure is a shortfall, so the direction is wrong. astubbs#337's
  committed-base/payload tear cannot shift what this test reads either - it passes base `0`
  explicitly to the deserialise call.
- **Not the retry-delay sleep.** That was the earlier diagnosis, attributed to astubbs#265 and
  reverted on astubbs#286 because the code it changes runs *after* the failing assertion. Recorded
  again here because the lesson generalises: match a fix to a stack line, not to a theme.
- **Not slowness.** See the second falsification arm above.

## Residual: the recorded actual of 132 is not explained by today's constants

The ledger recorded two actuals, 136 and 132. The mechanism predicts the frozen frontier is the top
of the last batch claimed before the block fired, so it is at or above the block point - with
today's constants that means 136, 137 or 138, and **132 is below the range the arithmetic allows**.
Reaching 132 requires an effective pressure threshold under 30 chars, i.e. a lower
`DefaultMaxMetadataSize` or `USED_PAYLOAD_THRESHOLD_MULTIPLIER` than this test sets. The failing
sha was never recorded, so the likeliest explanation is an older tree. Both statics are mutated by
this class and by `OffsetEncodingBackPressureUnitTest` and restored by both (`finally` and
`@AfterAll` respectively), which is why the class is `@Isolated`; nothing in the tree today leaks
them. The fix removes the sensitivity either way - the assertion no longer depends on where the
threshold falls.

## Sweep for other instances of the class - none found

The class is *a test asserting a count of records **sent** against a value the system derives from
records **succeeded**, in a scenario where a mechanism deliberately stops the succeeded frontier
reaching the top*. Every `getHighestSeenOffset` / `getOffsetHighestSeen` assertion in the test tree
was read:

- **`OffsetEncodingBackPressureUnitTest`** - the closest possible neighbour: the same scenario, the
  same `numberOfRecords + extraRecordsToBlockWithThresholdBlocks - 1` expression. **Not an
  instance**, for two independent reasons, and this is why it never flaked while its integration
  twin did. It asserts `getOffsetHighestSeen()`, which registration raises and back pressure does
  not gate; and it succeeds every extra record synchronously *before* triggering the encode, so
  there is no claim boundary to land in.
- **`WorkManagerOffsetMapCodecManagerTest`, `OffsetEncodingTests`, `DeltaListEncodingTest`,
  `BitSetEncodingTest`, `RunLengthEncoderTest`** - codec-level, no back pressure in play, and they
  compare the decoded value against `highestSucceeded` by name rather than a sent-record count.
  Not instances.
- **`OffsetEncodingTests#assertDegradedReloadedState`** - checked because it asserts seen and
  succeeded side by side; it distinguishes them deliberately and correctly. Not an instance.

## Prevention

- **When a test asserts a value produced by the mechanism it is testing, check the mechanism is not
  the thing stopping that value being reached.** Here the assertion wanted a frontier that the
  awaited event exists to freeze.
- **Put a constant where it is true.** `139` is a real, deterministic property of this scenario -
  of the *seen* offset, not the *succeeded* one. Relocating it beat both deleting it and asserting a
  range.
- **A rule-1 quarantine exception is a pressure release with a diagnosis as its open task, not a
  resolution.** This entry sat unowned and undiagnosed for weeks precisely because the pressure that
  would have forced the work had been released; what finally closed it was reading the mechanism in
  the source before running anything.
- **A pass rate near 100% is not evidence an expectation is reachable by design.** This one passed
  41 of 45 times purely because the mock consumer usually delivers the whole batch in one poll.
- **This entry is why rule 1 of the quarantine registry changed.** Under the old wording - *no
  quarantine without diagnosis* - an undiagnosed test stayed in the gating lane, so this one (4/45,
  the most frequent tracked flake) blocked every unrelated PR and had to be quarantined as an
  explicit owner-granted exception. Rule 1 now accepts a sighting ledger in place of a mechanism,
  which is what this test had; the provenance is recorded here because the ledger entry that carried
  it is deleted when the fix lands. Rule text: `docs/quarantined-tests.md`, grep
  `No quarantine without evidence`.
