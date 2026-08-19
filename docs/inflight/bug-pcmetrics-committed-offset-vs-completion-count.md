# `PCMetricsTest.metricsRegisterBinding` asserts something UNORDERED mode cannot guarantee

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->

**Not a flake, and not confluentinc#857.** A red here looks like a product defect and is not one -
which is why this is `misdirection` rather than a test-infra nit.

## The failing assertion

Seen on `fix/909-load-reproduction` CI, run 32244188439 (2026-08-19, Unit Tests):

```
expected: 1214.0 but was: 1207.0 within 2 minutes
```

Grep `PARTITION_LAST_COMMITTED_OFFSET, 1` in `PCMetricsTest` - the assertion is

```java
assertThat(registeredGaugeValueFor(PCMetricsDef.PARTITION_LAST_COMMITTED_OFFSET, 1))
        .isEqualTo(counterP1.get() + p1StartingOffset);
```

## Why it cannot hold

- `counterP1` counts **completions**. `LAST_COMMITTED_OFFSET` is bounded by the **lowest incomplete**
  offset, because commits are contiguous.
- The suite runs `UNORDERED` - grep `.ordering(UNORDERED)` in
  `AbstractParallelEoSStreamProcessorTestBase` - so records complete out of order by design.
- The two are equal only when completions happen to arrive in offset order. `1214` vs `1207` is 214
  completions against 207 contiguous: 7 records finished past a gap.
- The gap is **permanent**, not slow. Workers call `latch.await()` *before* `counter.incrementAndGet()`,
  so a latched worker's offset never completes. No `atMost` budget can close it; the 120s wait only
  makes the failure expensive.

So the test passes when the latched workers happen to hold the highest offsets, and fails otherwise.
The failure rate is a property of the interleaving, not of load.

## What it is NOT

**Not a confluentinc#857 sighting.** Recorded here because it was briefly mis-attributed as one. Every
sighting in that family is a chaos/rebalance *integration* test with a commit mode and a chaos seed
(`ChaosRevokeUnderWork*`, `ChaosChurnStormIT`, `RebalanceEoSDeadlockTest`). This is a MockConsumer
**unit** test with no broker, no rebalance, no revoke path and no commit mode - none of the family's
discriminators apply. `bug-857-family.md` already records one contamination of exactly this kind (a
transactional-mode failure logged as confirmation of a cycle impossible in that mode); this note
exists so the same mistake is not made a second time.

**Not a quarantine candidate.** Rule 1 asks for evidence, and the evidence here is a diagnosis - so
the answer is to fix the assertion, not to defer it. Quarantining a test whose expectation is simply
wrong would park a one-line repair behind the release guard.

## The fix

Assert against a metric that shares the assertion's own semantics. `PARTITION_HIGHEST_SEQUENTIAL_SUCCEEDED_OFFSET`
is the contiguous high-water mark and is what `LAST_COMMITTED_OFFSET` actually tracks; the completion
counter is the wrong comparand. The sibling assertions immediately below on
`PARTITION_HIGHEST_COMPLETED_OFFSET` and `PARTITION_INCOMPLETE_OFFSETS` are derived from the same
counters and want re-reading for the same confusion before any of them is changed.

**Do not "fix" this by widening the timeout or retrying** - the gap is permanent, so both would only
buy a slower red. Established from the code, not from a passing run: see AGENTS.md, Testing.
