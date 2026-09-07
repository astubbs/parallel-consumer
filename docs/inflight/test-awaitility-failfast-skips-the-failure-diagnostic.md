# Awaitility's `failFast` exit skips the diagnostic the `catch` was written for

<!-- inflight-type: bug -->
<!-- inflight-impact: blind-spot -->

`waitAtMost(...).failFast(...)` has **two** failure exits and they are unrelated siblings under
`RuntimeException`: the deadline throws `ConditionTimeoutException`, the fail-fast arm throws
`TerminalFailureException`. A `catch (ConditionTimeoutException)` therefore runs on the timeout and
**not** on the fail-fast path - so any reporting, dumping or diagnosis in that block is silently
absent for the one failure that already knows its own cause.

Nothing goes red. The test still fails; it just fails without the thing somebody wrote the `catch`
to produce, and the absence looks exactly like a test that had nothing to say.

## Where it still is

Found by a defect-class sweep off the first instance, `MultiInstanceHighVolumeTest`, where the
skipped block was the on-failure `PC-THROUGHPUT` figure - so `bin/performance-test.sh` printed
`NONE FOUND` for a performance run that had just measured a processor death, which is precisely the
case an on-failure measurement exists for. That one is fixed; `git log -S TerminalFailureException`
finds the change and its reasoning. Two instances remain:

- `BrokerIntegrationTest.awaitWithTopicNudge` - `failFast(pc::isClosedOrFailed)`, and the catch calls
  `logTailPositionDiagnosis`, the broker-versus-committed comparison that makes offset-reset
  positioning races self-identifying in CI logs. It is a base-class helper, so this is the widest
  instance: every IT that awaits through it loses that diagnosis when PC dies.
- `TransactionAndCommitModeTest` - `failFast("PC died, check logs.", ...)`, and the catch logs the
  expected/consumed/produced sizes and the **missing keys**, which is the whole diagnosis for that
  test.

Checked and dismissed: `VeryLargeMessageVolumeTest` has the same shape but its catch only re-fails
with the alias text, and the fail-fast exception already carries a clearer message ("PC died - check
logs"), so nothing is lost. `VertxConcurrencyIT` and the vertx/reactor/mutiny waits use no
`failFast` on the guarded await at all.

## The fix, when someone takes it

Add a second catch that reports and rethrows unchanged - report-and-rethrow, never report-and-swallow,
because the fail-fast arm is a genuine failure:

```java
} catch (TerminalFailureException e) {
    <the same diagnostic the timeout branch runs>
    throw e;
}
```

Left for a change of its own rather than folded into the one that fixed the first instance: both
files were untouched by it, and widening a merge-ready diff into two unrelated integration tests buys
a review round for a defect that has been latent for as long as the `failFast` calls have.
