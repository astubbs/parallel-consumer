# What this repo does not prove about behaviour across a crash, the offset map included

<!-- inflight-type: bug -->
<!-- inflight-impact: blind-spot -->

**The clean-close shape is proved end to end. The crash shape is not proved at all.** Every test in
the tree that demonstrates "committed work does not come back" reaches its committed state through an
orderly `close()` that flushes a final commit. Nothing asserts what a *successor* is handed after an
instance that committed a frontier died without closing.

This note is the gap, not the fix. A separate worker is recording the harness that would close part
of it as a next-milestone plan entry; cross-reference that rather than restating it here.

Every claim below comes from reading the assertion, not the method name. `grep -rn '<method>'
parallel-consumer-core/src` finds each one.

## The property has two halves, and they are not equally covered

1. **The uncommitted tail comes back.** Work that was in flight or failed when the instance died must
   be redelivered.
2. **Nothing below the committed frontier comes back.** Work the previous owner completed and
   committed must never be handed to the user function again.

Half 1 is the half the suite was written around. **Half 2 is asserted in exactly two places, both of
them clean-close or in-memory**, and in neither of them after a crash.

## What is proved today

- **`CloseAndOpenOffsetTest.offsetsOpenClose`** - the strongest evidence in the tree, and it proves
  **both halves**. Records `0..5` on one partition, `2` and `4` fail; the test waits for the commit
  to land, calls `closeDontDrainFirst()`, starts a second instance in the same group, and asserts
  `containsExactlyInAnyOrder("2", "4")` on what the successor processed. That fails if `0` or `1`
  (below the frontier) returns, and equally if `3` or `5` - which succeeded *above* the frontier and
  survive only in the encoded incomplete-offset map - returns. **Broker-only** (failsafe,
  Testcontainers). **Clean-close shape**: `closeDontDrainFirst` is still an orderly close with a final
  commit. Its `assumeFalse(skip.contains(encoding))` drops several `OffsetEncoding` values, catalogued
  in `docs/test-hardening/inactive-tests-audit-2026-08-08.md`.
- **`CloseAndOpenOffsetTest.correctOffsetVerySimple`** - the most rigorous *method* in the tree: it
  anchors on assignment first, asserts empty under a `pollDelay`, then produces a fresh record and
  asserts it IS read, which is a real control arm proving the empty observation came from a live
  consumer. **Broker-only, clean close, and the trivial case** - one record, no incompletes, nothing
  above the frontier.
- **`OffsetEncodingTests.ensureEncodingGracefullyWorksWhenOffsetsAreVeryLargeAndNotSequential`** -
  the only **unit-level** proof, no broker. It commits through a `MockConsumer`, then builds a fresh
  `WorkManager` from the committed state and asserts `containsExactlyElementsIn(expected).inOrder()`
  on what the new manager hands out, plus an explicit degraded-codec branch. This proves the
  **above-frontier** half of the offset map: records that succeeded above the committed offset are not
  re-offered. It does **not** prove half 2: the test filters its own re-poll with
  `x.offset() >= FIRST_COMMITTED_OFFSET`, so it *assumes* the broker seek rather than asserting it.

## What is not proved

- **The crash shape, at all - uncovered.** No test kills, halts or abandons an instance that has
  periodically committed a frontier with completed work still encoded above it, and then asserts on
  what the successor's user function is invoked with. `grep -rn 'SIGKILL\|System.exit\|halt('
  parallel-consumer-core/src/test-integration` returns nothing: even the chaos suite's
  `STOP_NO_DRAIN` resolves to an ordinary `close()`. There is no ungraceful-exit shape in the tree to
  build on.
- **Half 2 after any revoke or handover - uncovered.** `RebalanceEoSDeadlockTest.noDeadlockOnRevoke`
  is a genuine revoke-under-work handover, and it asserts zero duplicates on the **output topic under
  `read_committed`** - an exactly-once-output property that only exists in
  `PERIODIC_TRANSACTIONAL_PRODUCER` mode, and that by construction cannot fail because an input record
  below the frontier was reprocessed. Broker-only.
- **Half 2 in the chaos suite - covered only as a tolerance, which is not coverage.**
  `ChaosScenarioBase`'s end-of-run ledger asserts no loss (`unique.containsAll(expectedKeys)`) and
  bounds duplicates by a `perDisturbanceAllowance` per disturbance. A committed record coming back is
  *legal* there by a wide margin, so no chaos scenario can fail on this property. Broker-only.
- **The one genuine abandonment does not test the input side.** `TransactionalCrashReplayIT` really
  does abandon attempt one mid-transaction and fence its producer, but its committed frontier is the
  priming record only, and its assertions are `keysProcessedMoreThanOnce()
  .containsAtLeastElementsIn(payloadKeys)` (half 1) plus an output-topic exactly-once check. **It
  already computes the set that would answer half 2 and never excludes the primed key from it.** That
  is the cheapest gap in this note to close. Broker-only.
- **A near-miss that reads as coverage and is not.**
  `CloseAndOpenOffsetTest.largeNumberOfMessagesSmallOffsetBitmap` restarts after many records with a
  few failures and asserts `.as("Contains only previously failed messages").hasSize(...)`. The
  description claims identity; the assertion checks size only, so the wrong records of the right
  number pass. Its collections are `ConcurrentSkipListSet`, which silently de-duplicates redeliveries
  before any assertion sees them - so it could not detect a duplicate even if it asserted on one.
- **Externally-rewritten offsets are not the crash shape.** `PartitionStateCommittedOffsetIT` moves
  the committed offset by hand through `alterConsumerGroupOffsets` with a bare `OffsetAndMetadata`,
  which *destroys* the encoded offset map rather than exercising it. It answers "somebody rewrote our
  group offset", not "we committed a frontier and died". Broker-only, with a flake history of its own.
- **Nothing in the vertx, reactor or mutiny trees touches any of this** - no close-and-reopen, no
  restart, no committed-frontier assertion.

## Any test closing these gaps must be unsatisfiable by the pre-crash state

**This is a requirement on the fix, not advice.** The exact mistake has already been made in this
project and caught by a reviewer rather than by the suite: two crash-restart tests drained the output
topic after the restart using a fresh consumer group, which defaults to reading from the earliest
offset, so **phase one's own durable output satisfied the phase-two assertion**. Both were green
whether or not the mechanism under test existed, and deleting the restart entirely would not have
turned either red.

The class and its repair are written up in
`docs/solutions/test-issues/a-restart-assertion-satisfiable-by-pre-crash-data-proves-nothing.md`.
**That write-up is branch-only - it is not on `origin/master`, so a working-tree grep will not find
it.** Read it with `node bin/inflight.mjs docs show <that path>`. In short: capture the output
boundary at the moment of the crash, then *assign and seek* past it so the reader is structurally
incapable of returning pre-crash data - do not subscribe and filter, and do not repair a vacuous test
by strengthening its assertion.
<!-- file-refs: N/A - the solutions write-up cited in this paragraph exists only on unmerged branches, which is the point being made about it; the command given is how to read it -->

Red-then-green during development does not discharge this: a vacuous test also goes red before the
feature exists, for a different reason.

## Why it is recorded as a blind spot

The gap is on the one property the offset-map encoding exists to provide. A commit covering a record
still in flight, or a successor replaying work the previous owner had committed, produces no
exception, no warning and no gap in the log. The suite currently reports the crash-safety property as
covered - the test names, and the requirement links, all say so - while the shape that would break it
is untested. That is an absent signal on a silent-data-loss property, which is worse than a known
hole.
