# Review follow-ups left open from the inactive-test work

<!-- inflight-class: blind-spot -->


A seven-reviewer pass over astubbs#264 found four P1s, all fixed there (two of them reproduced by *running*
the tests: a 3-in-10 tick-path flake and a 1-in-10 worker-scheduling race). These are the findings that were
deliberately **not** folded into that PR, because each is a change to shared machinery rather than a fix to
the work under review.

## The frontier assertion should live in the shared test base

`ParallelEoSStreamProcessorTest` has private `awaitFrontier(int partition, long expected)` and
`highestCommitFor(TopicPartition)`. Every sibling in that family - `assertCommits`, `assertCommitLists`,
`awaitForCommit`, `awaitForCommitExact(int)` - lives in `AbstractParallelEoSStreamProcessorTestBase`, and is
reused across modules (`VertxTest` calls straight into it).

The irony is recorded in the learning this work produced
([`assert-the-commit-frontier-not-the-tick-path`](../solutions/test-flakiness/assert-the-commit-frontier-not-the-tick-path.md)):
that doc's whole thesis is that astubbs#260's rule failed to generalise because it lived somewhere you only
read if you already held the helper - and the fix for it was then put somewhere only one test class can reach.

Move both to the base, next to `awaitForCommit`, building on the already-mode-agnostic `getCommitHistory()`
that `highestCommitFor` calls anyway. Two blind alleys, already checked so they are not re-explored:

- **`awaitForCommitExact(int, int)`** was the obvious existing home and was deleted in astubbs#264 as dead. It
  verified `sendOffsetsToTransaction` on the producer spy, so it only ever worked under
  `PERIODIC_TRANSACTIONAL_PRODUCER` - useless for a test parameterised over all three commit modes.
- **`hasCommittedToPartition(tp)`** (the truth-subject path used by `MultiTopicTest`, `MutinyPCTest`,
  `TransactionTimeoutsTest`) reads the mock **consumer's** history, and under transactional mode commits go to
  the *producer*. That is precisely why `getCommitHistory()` dispatches on commit mode.

Neither spans the three modes. The new helper does, which is why it exists rather than reusing them.

## The frontier's negative checkpoints sample rather than hold

`await().untilAsserted` returns on its first passing poll, so a checkpoint that asserts "this partition has
**not** advanced" proves only the instant it ran. The exact-history form it replaced constrained continuously,
and the `verify(producerSpy, after(verificationWaitDelay).never())` it replaced before that supplied a real
settle window.

Concretely: a commit landing strictly between two checkpoints - say partition 1 committing 5 or 6 while record
4 is still in flight - is invisible, because `max` at the later checkpoint is the same either way. Committing
past in-flight work is exactly what `processInKeyOrder` exists to catch.

The fix is a holding variant used only at the "must not advance" checkpoints, keeping `awaitFrontier` for the
rising ones:

```java
private void awaitFrontierAndHold(int partition, long expected) {
    awaitFrontier(partition, expected);
    var tp = new TopicPartition(INPUT_TOPIC, partition);
    await().during(ofMillis(verificationWaitDelay)).atMost(defaultTimeout)
            .untilAsserted(() -> assertThat(highestCommitFor(tp)).hasValue(expected));
}
```

`verificationWaitDelay` is two commit intervals - the same budget the deleted `never()` form had. This restores
the settle without reintroducing the tick-path sensitivity the frontier change removed. Do it with the hoist
above, not separately.

## `CompletionCeiling`'s overflow guard is stricter than `Duration` needs

The guard tests whether `units * ceilingNanos` overflows a long, but the value produced is
`units * ceiling / gatingUnits`, and `Duration.multipliedBy` is `BigDecimal`-based. So it rejects knob values
whose *result* is perfectly representable, and blames the operator: `-Dvolume.messages=100000000` against the
120s site throws `"size ... is too large to scale a PT2M ceiling - check the knob"` for a valid 12,000s
deadline. The documented rungs (400k, 2M, 10M) all sit well under it, so nothing configured today trips it -
roughly 15x headroom above the largest.

Guard the operation that can actually overflow (catch `ArithmeticException` around the `Duration` arithmetic
and rethrow with the same friendly message) rather than a nanosecond proxy. Also add a positive-`ceilingAtGating`
check: `Duration.ZERO` currently divides by zero *inside* the guard meant to prevent raw arithmetic errors.
`CompletionCeilingTest` pins only the far side of the boundary, so the false-rejection band is untested.

## `CompletionCeilingTest` does not mirror the one inverted call site

The test's own javadoc says it "mirrors the real call sites, so a change to any of their constants breaks here
first", and it pins three of four. The one it skips is `TransactionAndCommitModeTest.timeoutFor`, the only site
whose arguments are inverted - `completionCeiling(GATING_CONCURRENCY, threads, defaultTimeout)` - because more
threads must *shorten*, not lengthen. That inversion is exactly where an argument-order slip is invisible, and
it is the site whose hand-rolled predecessor was provably wrong. Add the fourth mirror, including a rung case.

## Inherit: the audit is a dated record and has drifted

`docs/test-hardening/inactive-tests-audit-2026-08-08.md` records
`grep -c "^- \[ \]" docs/quarantined-tests.md  # 0`; that returns **3** today, because the registry gained
entries after 2026-08-08. Left as-is deliberately - a dated record's claims are not rewritten, only references
that no longer resolve (`docs/citations.md`). Re-run its reproduction commands rather than trusting the
numbers, and read the date first.

A reviewer also flagged that the corrections made to that audit were written **into** the dated file rather
than into a new dated audit, so it now carries later conclusions under an earlier date. Worth a decision if
that file is edited again.
