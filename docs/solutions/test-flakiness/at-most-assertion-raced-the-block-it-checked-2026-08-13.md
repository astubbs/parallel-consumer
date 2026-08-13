---
title: "An at-most assertion that raced the block it was checking: TransactionTimeoutsTest.produceTimeout waited a flat 5s against a 5s commit hold"
date: 2026-08-13
category: test-flakiness
module: parallel-consumer-core
problem_type: flaky_test
component: testing
symptoms:
  - "TransactionTimeoutsTest.produceTimeout: ConditionTimeoutException from BrokerCommitAsserter, `headOffset expected to be at most: 4, but was: 8`"
  - "Passes in isolation and on rerun; measured 1/20 plus one high-CPU sighting (2026-07-30), then 0/20, 0/3 and 0/45 across three reproducers (2026-08-07)"
  - "Nothing wrong in PC in the failing window - the records it committed had genuinely succeeded"
root_cause: assertion_window_raced_the_artificial_block_it_asserted_against
resolution_type: test_fix_anchor_assertion_to_the_blocked_window
severity: low
status: "SOLVED - test-side fix. PC is healthy; no product defect. The ledger's previously-recorded suspect (a lower-base commit splitting offsets 5 and 6) was NOT the mechanism and was never observed in 9 instrumented runs; it remains open by construction and is noted at the trigger site."
last_updated: 2026-08-13
related_prs:
  - "astubbs#220 - fixed the sibling commitTimeout in this same file, and is where this flake was left on the ledger as unfinished business"
related:
  - "docs/inflight/test-load-tightness-flakes.md - the family this belonged to; entry shrunk to point here"
  - "unforceable-trigger-commit-lock-timeout-2026-08-07.md - the sibling in the same file. Its 'Explicitly NOT an instance' section is correct that this test latches its TRIGGER, but its conclusion that the 'tight assertion' label stands is only accidentally right - the tightness is nowhere near the assertion's threshold"
  - "docs/investigating.md - the control-arm method this was settled with"
---

# The assertion and the thing it was asserting against were both 5 seconds long

`produceTimeout`'s phase 2 asserts that while a commit is artificially blocked, no new output records
become visible to a `READ_COMMITTED` consumer. Both halves of that were five seconds, and nothing tied
them together.

- The block: the injected `sendOffsetsToTransaction` holds the commit write lock and sleeps **5s**.
- The check: `BrokerCommitAsserter#assertConsumedAtMostOffset` waited a flat **5s**, then polled once.

The two windows started at different moments and only overlapped by luck. The block starts when PC's
own commit cadence reaches a commit whose base offset is `OFFSET_TO_PRODUCE_SLOWLY`; the check starts
whenever the test thread arrives. So the entire margin was

```
margin = (commit tick, relative to the check starting) - (the assert consumer's poll latency)
```

## Measured, not inferred

Instrumented run on an idle box, times relative to the at-most check starting:

| Event | t |
|---|---|
| offsets 5, 6, 8 all finish processing | +0 (same millisecond) |
| commit tick, `base=7`, 5s block begins | +1016 ms |
| at-most check polls and passes | +5512 ms |
| blocked transaction actually commits | **+6016 ms** |

**Margin: ~500 ms.** Both terms in it are wall-clock latencies - `produceMessages(4)` plus the assert
consumer's `subscribe`/group-join/first `poll` - so a contended runner or a slow broker erodes them
directly. When the margin goes negative the transaction commits *inside* the assertion's own poll
window, and the failure reads like an EOS violation while PC is behaving correctly.

## Control arm

The same 700 ms of latency, in two positions, everything else identical:

| Arm | 700 ms placed | block begins at | check completes | margin | Result |
|---|---|---|---|---|---|
| force | after the send, before the check | +320 ms | +5515 ms | **-180 ms** | **3/3 fail** |
| control | before the send | +1016 ms | +5514 ms | +502 ms | **3/3 pass** |

Same magnitude, different position, outcome flips - which is what rules out "it is just slower under
load". CPU contention alone does *not* reproduce it: under `SOAK_FREE_CORES=1` the margin stayed at
504-522 ms across runs, because burners slow both terms together.

## The fix

Anchor the check to the **start** of the block instead of hoping to overlap it. The test already has a
latch counted down as the injected commit begins its hold, while the write lock is held; phase 2 now
awaits that latch and checks with a delay sized against the hold (1s inside a 5s block). Margin goes
from ~0.5s to ~4s, and - the real point - the assertion now runs while the property it asserts is
actually claimed to hold.

`assertConsumedAtMostOffset` takes the delay as a parameter rather than hard-coding 5s. It has exactly
one caller, and the delay is a property of whatever is blocking the records, which only the caller
knows.

Verified:

| Arm | Result |
|---|---|
| fixed, stock settings | 3/3 pass |
| fixed, plus the 700 ms that failed the old assertion 3/3 | 3/3 pass |
| fixed, check delay moved back outside the block (7s vs 5s hold) | **2/2 fail** |
| whole `TransactionTimeoutsTest` class, fixed | see PR |

## What didn't work, and one refuted prediction

- **A single-test CPU soak.** 2 runs at `SOAK_FREE_CORES=1` before it was stopped as redundant; the
  ledger had already recorded 0/20 for this shape. The margin is not CPU-sensitive - burners dilate the
  commit tick and the poll latency together, leaving the difference intact. This is why three earlier
  reproducers all came back clean.
- **A guard asserting the block was still held when the check finished.** Written, then removed. The
  negative control showed the at-most assertion fails *first* (`headOffset ... but was: 10`), so the
  guard was never reached - an assertion nobody has seen fail is decoration
  (`docs/investigating.md`). The negative control was kept, because it verifies the anchor itself.

## What this does NOT fix

The ledger's previously-recorded suspect: the injected sleep is keyed on the commit base offset being
*exactly* `OFFSET_TO_PRODUCE_SLOWLY`, which assumes offsets 5 and 6 both complete between two commit
ticks. A tick landing between them commits for real with a lower base, and the at-most check would lose
even when anchored. **This was never observed** - zero such commits across 9 instrumented runs, and in
the baseline runs those two records completed in the same millisecond - so it is left as-is, noted at
the trigger site. Closing it would mean firing the slow commit on the first commit carrying any phase-2
progress rather than on an exact base offset.
