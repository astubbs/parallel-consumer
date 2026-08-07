---
title: Transactional (EoS) Mode Battle Test - Plan
type: test
date: 2026-08-07
artifact_contract: ce-unified-plan/v1
artifact_readiness: implementation-ready
product_contract_source: ce-plan-bootstrap
execution: code
---

# Transactional (EoS) Mode Battle Test - Plan

## Goal Capsule

**Objective.** Prove or falsify every guarantee this repo documents for
`CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER`, and subject the mode to adversarial load. A refuted
claim is a first-class result. Whether it ends in a corrected javadoc or a filed defect is a triage
decision, not a foregone conclusion - see R3.

**Authority hierarchy.** `AGENTS.md` outranks this plan on process. `CONCEPTS.md` owns vocabulary -
use *produce lock*, *commit lock*, *dirty*, *eager processing during commit*, *control loop*. The
javadoc under the `transactionalJavadoc` asciidoc tag and the `[[transaction-system]]` section of
`src/docs/README_TEMPLATE.adoc` are jointly the claim source of truth; `README.adoc` is generated
from both and is never hand-edited.

**Stop conditions.** Stop and report rather than guessing when: a claim test goes red in a way that
implicates main code (that is the deliverable, not a blocker); the chaos scenario needs an SLO
loosened to pass; or a fix would touch files owned by the in-flight branches named in KTD1.

**Execution profile.** Evidence-first. Every claim declared proved carries a negative control that
was *seen* to fail. Every rate is reported as `n/N` with the load conditions, never as a verdict. A
measurement tool that could report success without running anything is not evidence - see KTD13.

---

## Product Contract

### Summary

Add a claim-anchored test suite for transactional mode, plus one chaos scenario that runs the mode
under rebalance churn. The suite is organised around a machine-checked register of the thirteen
claims the documentation makes, checked in both directions so neither coverage nor the recorded
claim text can silently rot.

### Problem Frame

Transactional mode makes thirteen distinct, testable promises to users. Existing tests cover the
lock protocol and both timeout paths well, and cover visibility only in part:
`TransactionTimeoutsTest#produceTimeout` already asserts the output topic at `READ_COMMITTED` in
both directions while a commit is blocked. What no test covers is abort, transaction timeout,
crash-before-commit, and a `read_uncommitted` control arm - so nothing proves an absence assertion
is non-vacuous, and nothing asserts a produced record is exactly-once *in Kafka*.
`TransactionAndCommitModeTest` counts callbacks rather than broker contents.

The mode is also the project's own rostered next chaos lever
(`docs/inflight/test-chaos-phase2.md`), and the one upstream issue ever labelled *verified bug*
(astubbs#44, confluentinc#803) is a transactional commit-lock stall whose fix is still unmerged.
Master is known-defective here, which makes calibration possible rather than hypothetical.

### Requirements

#### The claim register

- R1. Every claim made by the `transactionalJavadoc` tag and by `[[transaction-system]]` in
  `src/docs/README_TEMPLATE.adoc` is enumerated as a named constant carrying the claim's verbatim
  sentence, so tests reference a claim rather than restating it.
- R2. The register carries a coverage status per claim. A claim whose status asserts coverage fails
  the build when no test references it. `KAFKA_GUARANTEE` and `NOT_YET_COVERED` are reported, not
  failed.
- R3. A refuted claim is first triaged as **doc-overreach** (the documentation promised more than
  the design ever intended) or **code-defect** (the design intended the guarantee and the code does
  not deliver). Doc-overreach is corrected in the documentation in this change. A code-defect leaves
  the documentation untouched and is recorded as refuted pending a fix, so a real bug is never
  resolved by quietly weakening the guarantee.
- R19. Every refuted claim and every defect this work discovers is filed as a GitHub issue and
  recorded in `docs/inflight/`, and any documentation correction links the issue in
  `astubbs/parallel-consumer#NN` form.
- R20. The register's recorded sentence for each claim is checked against its source text, so an
  edit to the javadoc or the README template that leaves the register stale fails the build.

#### Claims proved in the unit lane

- R4. Records produced by different workers in one commit interval land in a single transaction
  (C1).
- R5. Selecting transactional mode without an explicit commit interval reduces the interval from 5s
  to 100ms (C5).
- R6. No record reaches the broker whose source offset is excluded from the transaction being
  committed (C9) - asserted under a forced overlap, not an incidental one.
- R7. Record processing is blocked for the duration of the commit-lock hold (C10).
- R21. The two timeout claims - commit-lock timeout fails fast with a final commit attempt (C11),
  and produce-lock timeout returns the record for retry (C12) - are attributed to the existing tests
  that already prove them, rather than reproved.

#### Claims proved in the integration lane

- R8. A `READ_COMMITTED` consumer observes none of a transaction's records until it commits, and a
  `read_uncommitted` control arm observes them - proving the assertion is not vacuous (C2, C6).
- R9. Records produced into a transaction that is aborted or times out are never visible at
  `READ_COMMITTED` (C8).
- R11. With `pollAndProduceMany`, every record of a batch becomes visible together or not at all
  (C7).
- R12. With eager processing during commit enabled, a record retried across a commit may replay its
  side effect; with it disabled, it may not (C13).
- R10. A source offset and its produced records commit as one atomic set: abandoning the instance
  before commit leaves neither, and the replayed work commits in a new transaction with no duplicate
  visible on the output topic (C3, C4).
- R22. The crash-and-replay arm uses a stable `transactional.id` so the restarted instance fences its
  predecessor. Without fencing the abandoned transaction pins the LSO until it times out, and the
  absence assertion passes for the wrong reason.

#### Claims proved under chaos

- R13. A chaos scenario runs transactional mode under rebalance and restart churn with KEY ordering
  and an explicitly chosen sub-second commit interval, and asserts exactly-once on the output topic
  read at `READ_COMMITTED`.
- R14. The scenario's output-side ledger allows zero duplicates, distinct from the input-side ledger
  which legitimately allows reprocessing duplicates per disturbance.
- R15. A commit-lock timeout is classified explicitly and counted, rather than being absorbed by the
  expected-close whitelist that swallows it today.
- R23. The scenario includes a second-instance-start arm, the shape of the one upstream-verified bug
  in this mode (astubbs#44, confluentinc#803), with its expected verdict stated per branch.

#### Evidence

- R16. Every claim declared proved carries a negative control whose failure was observed.
- R17. Results report reproduction rate as `n/N` with load conditions, measured with a tool that
  actually selects the test.
- R18. Where the claim tests are calibrated against a known real defect, the plan records the RED
  arm and the GREEN arm by commit.

### Scope Boundaries

**In scope.** Test code, the chaos-harness extension needed to run EoS, the claim register,
documentation corrections for doc-overreach refutations, issue filing for code-defect refutations,
and this plan's results write-up.

**Not goals.** Fixing main-code defects the suite discovers. A red test that implicates main code is
the intended output; the fix is a separate PR with its own diagnosis. R3 and R19 make sure that
output is recorded rather than lost.

#### Deferred to follow-up work

- The `ProducingLock` batch double-release defect and its two unit tests. Owned by the in-flight
  branch named in KTD1.
- Producer fencing as a general harness capability. `KafkaClientUtils` assigns a unique random
  `transactional.id` per producer, and changing that default would affect every existing
  transactional test. R22 scopes a stable id to the crash-and-replay arm only - it does not change
  the shared default.
- `ConsumerOffsetCommitter#commitAndWait` misreporting its elapsed wait by interpolating
  `DEFAULT_TIMEOUT` instead of the configured commit timeout.
- The latent flaw in the `CommitFailedException` handler that `docs/plans/2026-08-01-001` §0.5
  records as unfixed: swallowing on that path marks offsets clean that never reached the broker.
- Correcting the stale `AGENTS.md` sentence claiming `ChaosChurnStormIT` is quarantined and that the
  CI chaos lanes select zero tests. Real (the registry records none quarantined) but unrelated to
  transactional mode, so it does not ride along in this PR.

### Sources

- Claims: `parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/ParallelConsumerOptions.java`
  lines 130-215 (`transactionalJavadoc` tag); `src/docs/README_TEMPLATE.adoc` `[[transaction-system]]`.
- Method: `AGENTS.md` "Settling it: a fix that works is not evidence of the cause".
- Seam pattern: `c429d8b6` / astubbs#220, and
  `docs/solutions/test-flakiness/unforceable-trigger-commit-lock-timeout-2026-08-07.md`.
- Invariant: `docs/plans/2026-08-03-001-investigate-transactional-commit-flake.md` §5, §11.
- Chaos roster: `docs/inflight/test-chaos-phase2.md`.
- Vacuity hazard: `docs/solutions/test-flakiness/vacuous-await-condition-brokerpoller-backpressure-2026-07-31.md`.
- Chaos failure classification: `docs/plans/2026-08-01-001-investigate-chaos-w4-red-report.md` §5, §7.4.
  Note KTD7: that report's account of the exception type does not match the current tree.
- Live defect: astubbs#44 (confluentinc#803); `docs/inflight/bug-857-family.md`.

---

## Planning Contract

### Key Technical Decisions

KTD1. **Do not re-implement the batch produce-lock fix.** astubbs#257
(`fix/produce-lock-double-release`) already fixes it and ships `TransactionalBatchProduceTest` and
`ProduceLockReleaseTest`. This plan consumes that PR as a calibration arm and adds neither test.
Rationale: another worktree owns those files; duplicating them collides on merge and splits the
diagnosis.

KTD2. **Calibrate against a real defect, not only synthetic controls.** Master with `batchSize >= 2`
under a transactional producer fails whole batches, and because only a success marks a partition
dirty it then stops committing entirely - a stall, which is more severe than the redelivery the fix
was written for. astubbs#257 fixes it: applying only its `src/main` files takes the batched arm from
one error in 178s to 5/5 passing in 72s. This satisfies the chaos suite's acceptance bar - "a chaos
suite that never caught a known bug is decoration" - with a bug that exists rather than one injected,
and with both halves of the pair actually run.

KTD11. **The produce-lock fix lands first; this suite does not merge a known-red gating test.**
`TransactionalCrashReplayIT` is an untagged `integrationTests` class, so `bin/ci-integration-test.sh`
runs it in the gating lane. A test the plan predicts fails on master would turn that lane red for
every subsequent PR. That fix is now astubbs#257, open against master, so the ordering is a
dependency rather than a hope: land it, then re-enable the arm. Until it lands, U13 ships with that
one scenario `@Disabled` naming astubbs#257, and its RED result is recorded in U11 as evidence rather
than as a gating test. Rejected: merging it red and "fixing it later" - that is how a lane becomes
ignorable. Quarantine was also considered and is the wrong instrument here:
`bin/check-quarantine-owners.sh` wants an owning PR that removes the annotation, and astubbs#257 is
someone else's PR fixing main code, not this suite's to carry.

KTD3. **A new chaos scenario, not a widened W4.** W4's legitimate lag-stagnation peaks sit at
117-123s against a 150s bound - about 1.25x headroom. Transactional commit overhead would trip that
bound for reasons unrelated to the defect class it detects. Rejected alternative: parameterise W4 by
commit mode; it would de-calibrate a working tripwire.

KTD4. **Resolve the transaction-timeout/dwell collision by making it a test, not a workaround.**
`KafkaClientUtils#createNewProducer` hardcodes `transaction.timeout.ms=10s`, while the chaos heavy
tail dwells 20-45s holding the produce lock - so today every heavy record would blow the transaction
timeout and fence the producer. Make the timeout a parameter, set it above the dwell for the main
scenario, and add one deliberate short-timeout arm that forces a transaction timeout to prove C8.
This tunes `transaction.timeout.ms` only; `commitLockAcquisitionTimeout` (5 min default) and
`produceLockAcquisitionTimeout` (1 min default) stay at their defaults unless U10 states otherwise,
so U9's SLO is derived from one moving part. Rejected: shrinking the dwell, which is what makes the
dwell and drain probes able to discriminate.

KTD5. **Prove non-vacuity before asserting absence.** Every "not visible" assertion first
establishes that the consumer was assigned and had seen at least one record. A verifying consumer
asserting "nothing visible" is trivially satisfied before assignment - the failure mode written up
in the vacuous-await solution doc.

KTD6. **Never derive record counts from offsets on a transactional topic.** Commit markers occupy
offsets, so end-offset arithmetic overstates record count. Count consumed records. This is also why
`ProgressProbe`'s lag sampler must not be pointed at the output topic.

KTD7. **The commit-lock tripwire is already blind; narrow it rather than avoid widening it.**
`ProducerManager#acquireCommitLock` throws `java.util.concurrent.TimeoutException`, which the
control thread wraps as the cause of a `RuntimeException`.
`ManagedPCInstance#isExpectedCloseException` walks the cause chain and returns true for any
`TimeoutException`, so a commit-lock timeout is classified as an *expected* close today and the
instance silently restarts. U9 must therefore narrow that blanket arm so the commit-lock timeout
reaches classification. This supersedes `docs/plans/2026-08-01-001` §5's account, which describes
the exception as a cause-less `InternalRuntimeException` the whitelist cannot match - that does not
match the current tree, and planning against it would have produced a unit built for an exception
that is never thrown.

KTD12. **Name the crash mechanism; the close paths cannot provide one.**
`AbstractParallelEoSStreamProcessor#innerDoClose` calls `commitOffsetsThatAreReady()` before
touching the producer, so `closeDrainFirst()` and `closeDontDrainFirst()` both commit the pending
transaction. The crash arm therefore abandons the instance without calling `close()` and lets the
configured `transaction.timeout.ms` expire the open transaction, then starts a fresh instance in the
same group with the same `transactional.id` (R22) so the newcomer fences the abandoned producer.
Rejected: a throwing seam inside the commit path, which would prove a different claim - an injected
commit failure, not "the system crashes before finishing sending".

KTD8. **Unit lane for lock and state claims, integration lane for every visibility claim.**
`MockProducer` models the transaction state machine but no broker-side visibility, no markers, no
isolation level and no transaction timeout. Any claim containing the word "visible" is broker-only.

KTD9. **Force triggers through `PCModule`, never around it.** Override `producerManager()` in an
anonymous `PCModule` subclass and **cache the instance** - the provider memoizes, and a fresh
`ProducerManager` would carry a fresh `producerTransactionLock`, so produce and commit locks would
stop being two sides of one lock.

KTD10. **Defer one record's success into the window under test.** `maybeAcquireCommitLock` is gated
on `wm.isDirty()`, whose only setter is `PartitionState#onSuccess` - `onFailure` is a no-op. If
every other record succeeds before the record under test starts, no commit is attempted at all and
there is no deadline to widen. `requestCommitAsap()` cannot override this because `isDirty` is
AND-ed into the gate.

KTD13. **`bin/soak-test.sh` measures integration classes only.** It passes `-Dit.test=<test>
-Dtest=SKIPNONE` with `-Dfailsafe.failIfNoSpecifiedTests=false`, and failsafe's includes are
`**/integrationTest*/**/*.java`. Pointed at a unit-lane class it selects nothing, exits zero, and
reports a clean `n/N` having run no test at all. Unit-lane rates are measured with a direct
`./mvnw -pl parallel-consumer-core -am test -Dtest='<Class#method>'` loop instead.

### High-Level Technical Design

The claims cluster on one protocol. This is what the tests observe, and where each claim is
falsifiable:

```mermaid
sequenceDiagram
    participant W as Worker (produce lock = read side)
    participant C as Control loop (commit lock = write side)
    participant P as Producer
    participant B as Broker
    participant V as Verifier (READ_COMMITTED)

    W->>W: acquireProduceLock (before user fn when eager=false)
    W->>P: send() - lazyMaybeBeginTransaction
    Note over W,P: C1 all workers share ONE open transaction
    W->>W: WorkContainer reaches controller mailbox
    W->>W: release produce lock (C9: not one instruction earlier)

    C->>C: preAcquireOffsetsToCommit -> acquireCommitLock
    Note over C,W: C10 workers now blocked
    C->>C: collectCommitDataForDirtyPartitions
    C->>P: sendOffsetsToTransaction + commitTransaction
    P->>B: commit
    C->>C: postCommit -> releaseCommitLock

    V-->>B: poll
    Note over V,B: C2/C6/C7 nothing visible before commit; all of it after
    Note over V,B: C8 aborted or timed-out transaction: never visible
```

The falsification target is the gap between "release produce lock" and "gather offsets". If a
release lands early, a transaction commits offsets that do not account for records already inside
it - the EoS violation. Its observable signature, recorded in
`docs/plans/2026-08-03-001` §11, is a `sendOffsetsToTransaction` issued after two `send` calls, with
PC's incomplete-offset encoding present in the commit metadata.

### Assumptions

- Phase A (U1-U6, U13, and U11's Phase A arms) is independently shippable. If Phase B's harness work
  proves larger than estimated, Phase A lands alone and Phase B becomes a follow-up. Recorded because
  no user was present to size this.
- The suite runs on the single shared static `KafkaContainer`; no multi-broker harness exists and
  none is built here.
- astubbs#257 merges before U13's no-duplicate arm is re-enabled (KTD11). It is open and mergeable;
  until it lands, U13 ships that arm disabled and U11 records both halves of the calibration - the
  RED on master and the GREEN observed by applying that PR's `src/main` files here.
- U1's coverage scan can see `@ProvesClaim` on integration classes because the root pom adds
  `src/test-integration/java` as a test source root of the same module via `build-helper`, so both
  lanes compile into one `target/test-classes`. This is load-bearing for the register.

### Sequencing

Phase A proves the claims. Phase B runs them under chaos. U11 gates the claim of proof for both and
splits by phase: its Phase A arms are due when Phase A lands and do not wait on U10. U13 is gated on
KTD11's landing rule.

---

## Implementation Units

| U-ID | Title | Key files | Depends on |
|---|---|---|---|
| U1 | Claim register + bidirectional coverage enforcement | `src/test/.../TransactionalClaim.java`, `ProvesClaim.java`, `TransactionalClaimCoverageTest.java` | - |
| U2 | Bulk-sharing and commit-interval claims (unit) | `src/test/.../internal/TransactionalBulkCommitTest.java` | U1 |
| U3 | Ordering-invariant and blocking claims (unit) | `src/test/.../internal/ProducerManagerTest.java`, `truth/ProducerManagerSubject.java`, `integrationTests/TransactionTimeoutsTest.java` | U1 |
| U4 | Visibility boundary (integration) | `src/test-integration/.../TransactionalVisibilityIT.java`, `utils/KafkaClientUtils.java` | U1 |
| U5 | Batch all-or-none via `pollAndProduceMany` (integration) | `src/test-integration/.../TransactionalBatchVisibilityIT.java` | U1, U4 |
| U13 | Crash-and-replay atomicity (integration) | `src/test-integration/.../TransactionalCrashReplayIT.java` | U1, U4; KTD11 landing rule |
| U6 | Eager-processing replay claim (integration) | `src/test-integration/.../TransactionalEagerProcessingIT.java` | U1, U4 |
| U7 | Harness: transactional `ManagedPCInstance` | `src/test-integration/.../utils/ManagedPCInstance.java`, `utils/KafkaClientUtils.java`, `chaostests/ChaosScenarioBase.java` | U4 |
| U8 | Output-side exactly-once ledger | `src/test-integration/.../chaostests/EosOutputLedger.java`, `EosOutputLedgerIT.java` | U7 |
| U9 | Commit-timeout failure classification | `src/test-integration/.../utils/ManagedPCInstance.java`, `chaostests/ChaosScenarioBase.java` | U7 |
| U10 | Chaos scenario W5: EoS under churn | `src/test-integration/.../chaostests/ChaosTransactionalEosIT.java` | U7, U8, U9 |
| U11 | Calibration arms and negative controls | plan results section | Phase A arms: U2-U6, U13. Chaos arm: U10 |
| U12 | Docs: results, corrections, issue filing | `ParallelConsumerOptions.java`, `src/docs/README_TEMPLATE.adoc`, `README.adoc`, `docs/inflight/` | U11 |

### U1. Claim register + bidirectional coverage enforcement

**Goal.** Make the thirteen documented claims addressable by name, make missing coverage a build
failure, and make a stale recorded sentence a build failure too.

**Requirements.** R1, R2, R20.

**Files.**
- `parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/TransactionalClaim.java` (new)
- `parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/ProvesClaim.java` (new)
- `parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/TransactionalClaimCoverageTest.java` (new)

**Approach.**
1. An enum with one constant per claim: `BULK_SHARED_TRANSACTION`, `ALL_OR_NONE_PER_SOURCE_OFFSET`,
   `FAILURE_INVISIBLE_AND_RECOMBINED`, `OFFSET_AND_RECORDS_ATOMIC`, `COMMIT_INTERVAL_AUTO_REDUCED`,
   `READ_COMMITTED_BLOCKED_TO_FIRST_OPEN_TX`, `PRODUCE_MANY_ALL_OR_NONE`, `ABORTED_NEVER_VISIBLE`,
   `NO_PRODUCE_WITHOUT_ITS_OFFSET`, `PROCESSING_BLOCKED_DURING_COMMIT`,
   `COMMIT_LOCK_TIMEOUT_FAILS_FAST`, `PRODUCE_LOCK_TIMEOUT_RETRIES_RECORD`,
   `EAGER_PROCESSING_MAY_REPLAY`. Add a constant for the README template's own promise that results
   exist exactly once in the output topic even under failure, if it does not reduce to an existing
   constant.
2. Each constant carries its verbatim source sentence, the source it came from (javadoc tag or
   README template), and a status:
   - `PROVED` - covered, with an observed negative control. Coverage is enforced.
   - `REFUTED` - tested and false. Coverage is enforced; disposition follows R3.
   - `COVERED_NO_CONTROL` - covered by a test, but no negative control was produced. Coverage is
     enforced; the claim does **not** count as proved and does not trigger R3.
   - `KAFKA_GUARANTEE` - the guarantee is Kafka's, surfaced by our docs. Reported, not enforced.
   - `NOT_YET_COVERED` - owned by a later phase, with a recorded reason. Reported, not enforced.
3. Tests declare coverage with a `@ProvesClaim(...)` annotation on the test method.
4. The coverage test runs both directions: every enforced constant has a referencing test, and every
   constant's recorded sentence still occurs verbatim in its source file's tagged region. Use the
   ArchUnit already on the test classpath rather than hand-rolled reflection.
5. Annotate the existing `TransactionTimeoutsTest#commitTimeout` and `#produceTimeout` for the two
   timeout claims (R21) rather than reproving them.

**Approach note - what this does and does not guarantee.** The coverage test proves a claim is
*referenced*, not that it is *proved*. An annotation on a weak test satisfies it forever. Proof
strength rests entirely on U11's recorded negative controls; say so in the class javadoc so a future
reader does not read green as evidence the claims still hold.

**Patterns to follow.** `TestConventionRules` for the ArchUnit idiom; `ProgressProbeLedgerIT` for a
broker-free test that deliberately gates every build.

**Test scenarios.**
- An enforced constant with no `@ProvesClaim` reference fails, naming the constant.
- A `KAFKA_GUARANTEE` or `NOT_YET_COVERED` constant with no reference is reported and does not fail.
- A `NOT_YET_COVERED` constant with no recorded reason fails - the status may not be used to park a
  claim silently.
- Editing a claim's sentence in `ParallelConsumerOptions.java` without updating the register fails,
  naming the constant and both texts.
- A `@ProvesClaim` on a method in a class that **neither** surefire nor failsafe would collect is a
  failure. Classes under `..integrationTest..` / `..integrationTests..` are exempt from the surefire
  naming rule because failsafe selects them by package - mirror
  `TestConventionRules#test_classes_must_be_named_so_surefire_collects_them`.

**Verification.** Deleting any single claim test turns the build red with that claim's name in the
message; editing a javadoc claim sentence does the same.

### U2. Bulk-sharing and commit-interval claims (unit)

**Goal.** Prove C1 and C5.

**Requirements.** R4, R5.

**Files.**
- `parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/internal/TransactionalBulkCommitTest.java` (new)

**Approach.** Drive `pc.controlLoop` manually with the `ProducerManagerTest#buildModule` pattern
(override `pc()` with `isTimeToCommitNow() -> true` and a no-op `close()`). Run several records
concurrently within one interval and assert one `beginTransaction` and one
`sendOffsetsToTransaction` covering all their offsets. For C5, assert the resolved options rather
than the builder input, and assert both directions: unset interval becomes 100ms, an explicitly set
interval is left alone.

**Patterns to follow.** `ProducerManagerTest#producedRecordsCantBeInTransactionWithoutItsOffsetDirect`.

**Trap.** `PCModuleTestEnv` injects a `Mockito.mock(Producer.class)`, not a `MockProducer`, so
`ProducerWrapper`'s reflective `txManagerField` is null and `isTransactionReady()` will NPE. Do not
call `producerManager.close(...)` from this test.

**Test scenarios.**
- Four records across two shards in one interval produce four `send` calls and exactly one
  transaction, whose `sendOffsetsToTransaction` carries all four offsets.
- A second commit interval opens a new transaction rather than reusing the committed one.
- Transactional mode with no explicit commit interval resolves to 100ms.
- Transactional mode with an explicit 2s interval resolves to 2s.
- Non-transactional mode with no explicit interval stays at 5s.

**Verification.** C1 and C5 marked covered; the four-records-one-transaction assertion fails when
the interval is forced short enough to split the batch.

### U3. Ordering-invariant and blocking claims (unit)

**Goal.** Strengthen C9 and C10 from incidental to forced, and attribute C11 and C12 to the tests
that already prove them.

**Requirements.** R6, R7, R21.

**Files.**
- `parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/internal/ProducerManagerTest.java` (modify)
- `parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/truth/ProducerManagerSubject.java` (modify)
- `parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/TransactionTimeoutsTest.java` (modify - annotations only)

**Approach.** Extend the existing class rather than adding a sibling - it already owns this lock's
unit coverage and carries the `@Tag("transactions")` convention. Use the astubbs#220 seam: two
latches, one signalling that a worker holds the produce lock, one signalling that the controller has
entered commit-lock acquisition, with the override cached per KTD9 and gated so happy-path commits
do not open it. Add `ProducerManagerSubject` assertions for produce-lock hold count and commit-lock
ownership so the invariant reads as an assertion rather than raw lock introspection. Add
`@ProvesClaim` to `TransactionTimeoutsTest#commitTimeout` and `#produceTimeout`; change no assertion
in that file.

**Execution note.** Per KTD10, drive at least one other record to success inside the overlap window
so `wm.isDirty()` is true and a commit is actually attempted. Without that there is no commit
attempt to block, and the test is vacuous rather than tight. Assert the latch reached zero after the
await, so a missed overlap names itself rather than presenting as a generic timeout.

**Patterns to follow.** `TransactionTimeoutsTest#commitTimeout` (`c429d8b6`);
`BlockedThreadAsserter` for the blocking half.

**Test scenarios.**
- While a worker holds the produce lock, `preAcquireOffsetsToCommit` blocks; it completes only after
  the worker's `WorkContainer` reaches the controller mailbox, not merely after its `send` returns.
- While the commit lock is held, `beginProducing` blocks, and unblocks on release
  (`BlockedThreadAsserter.assertUnblocksAfter`).
- The produce lock is still owned by the context when the user function returns - the guard from
  `docs/plans/2026-08-03-001` §11.
- A commit whose offset map is gathered while a produce lock is held never issues
  `sendOffsetsToTransaction` before that lock is released.
- The dirty gate is satisfied deliberately: the test fails loudly if no commit was attempted, rather
  than passing because nothing happened.

**Verification.** Negative control per U11: move the produce-lock release to before the mailbox
handoff and confirm the invariant test fails deterministically.

### U4. Visibility boundary (integration)

**Goal.** Prove C2, C6 and C8 against a real broker.

**Requirements.** R8, R9.

**Files.**
- `parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/TransactionalVisibilityIT.java` (new)
- `parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/utils/KafkaClientUtils.java` (modify)

**Approach.** Extend `BrokerIntegrationTest`. `KafkaClientUtils#setupConsumerProps` already sets
`isolation.level=read_committed`, so the verifier needs no configuration; the *control* arm is the
one that must opt into `read_uncommitted` via `createNewConsumer(groupId, Properties)`. Add a
transaction-timeout parameter to `createNewProducer` (KTD4), defaulting to today's 10s so existing
callers are unchanged, and an optional stable `transactional.id` for U13's use (R22) - also
defaulting to today's random id.

**Execution note.** Establish non-vacuity before every absence assertion (KTD5): assert the verifier
holds a non-empty assignment and has consumed at least one earlier committed record before
asserting that the in-flight transaction's records are absent.

**Patterns to follow.** `TransactionTimeoutsTest`'s use of a `NEW_GROUP` `READ_COMMITTED` consumer;
`KafkaClientUtils#createAndInitNewTransactionalProducer` for raw broker-side transactions, as used
by `TransactionMarkersTest`.

**Test scenarios.**
- Records produced into an open transaction are absent at `READ_COMMITTED` and present at
  `read_uncommitted` - the control arm that proves the absence is real.
- After commit, all of that transaction's records become visible at `READ_COMMITTED` in one step.
- An aborted transaction's records are never visible at `READ_COMMITTED`, before or after the abort.
- A transaction that exceeds `transaction.timeout.ms` leaves no visible record.
- With one transaction still open and a later one committed, a `READ_COMMITTED` consumer sees
  neither - consumption is blocked at the first still-open transaction, not merely filtered.
- Vacuity guard: the same absence assertion run against a consumer with no assignment fails.

**Verification.** C2, C6, C8 covered; the `read_uncommitted` arm observed to see what the
`READ_COMMITTED` arm does not.

### U5. Batch all-or-none via `pollAndProduceMany` (integration)

**Goal.** Prove C7 - a multi-record result set is visible together or not at all.

**Requirements.** R11.

**Files.**
- `parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/TransactionalBatchVisibilityIT.java` (new)

**Approach.** Produce several result records per input record and assert the output topic at
`READ_COMMITTED` never shows a partial set. Count consumed records, never offsets (KTD6).

**Test scenarios.**
- Every input offset's produced records appear together on the output topic or not at all.
- A `pollAndProduceMany` batch where one send fails terminally leaves none of that batch visible.
- Worker-thread exceptions are captured and asserted on: nothing in main reads
  `WorkContainer#future`, so an exception on the worker path is otherwise invisible.

**Verification.** C7 covered, with a negative control per U11.

### U13. Crash-and-replay atomicity (integration)

**Goal.** Prove C3 and C4 - the claims users buy EoS for.

**Requirements.** R10, R22.

**Files.**
- `parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/TransactionalCrashReplayIT.java` (new)

**Approach.** Per KTD12, abandon the instance without calling `close()` - both close paths commit
first, so neither can express a crash - and let `transaction.timeout.ms` expire the open
transaction. Restart into the same group with the **same** `transactional.id` (R22) so the newcomer
fences the abandoned producer and its records resolve as aborted rather than pinning the LSO. Read
the output topic at `READ_COMMITTED` and compare the multiset against the input: input-side
reprocessing is legal and expected; an output-side duplicate is a claim violation.

**Execution note - landing rule (KTD11).** The no-duplicate arm is expected RED on master at
`batchSize >= 2`. It may only merge once `fix/produce-lock-double-release` is on master. Until then
it ships disabled behind a named reference to the blocking commit, and U11 records the RED result as
evidence. Do not merge it red into the gating lane. Volume matters: the defect's own write-up reports
3/3 redelivery at 6 records and 5/5 at 200.

**Patterns to follow.** `TransactionAndCommitModeTest` for volume-and-parity shape; its gap is that
it counts callbacks rather than broker contents, which is exactly what this unit adds.

**Test scenarios.**
- Abandoning the instance after `send` but before commit leaves no output record visible at
  `READ_COMMITTED`, and the fencing of the abandoned producer is asserted rather than assumed.
- The replayed work commits in a new transaction, observed without waiting out an unfenced timeout.
- Across that replay the output topic contains each expected value exactly once, while the input
  side may show reprocessing.
- The recombination claim: the retry transaction's record set is a valid set for the replayed
  offsets, not necessarily identical to the original grouping.
- Non-vacuity: the verifier is proven to have been assigned and to have seen committed records
  before the absence assertion runs.

**Verification.** RED on master, GREEN on the merged produce-lock fix, both recorded in U11 with
`n/N`.

### U6. Eager-processing replay claim (integration)

**Goal.** Prove C13 - the documented trade-off of
`allowEagerProcessingDuringTransactionCommit`.

**Requirements.** R12.

**Files.**
- `parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/TransactionalEagerProcessingIT.java` (new)

**Approach.** Count user-function invocations per input record with an external side-effect counter
while forcing a produce-lock timeout during a commit. The claim is directional, so the test asserts
the difference between the two settings rather than an absolute count.

**Test scenarios.**
- Eager disabled: a record retried across a commit does not re-run its side effect.
- Eager enabled: the same record may re-run its side effect, and the test asserts the observed
  replay rather than merely permitting it.
- Eager disabled: the produce lock is taken before the user function runs.
- Eager enabled: the produce lock is taken after the user function returns.

**Verification.** C13 covered, with the two arms differing on the same forced trigger.

### U7. Harness: transactional `ManagedPCInstance`

**Goal.** Let a chaos scenario select transactional mode at all, at the settings it claims.

**Requirements.** R13.

**Files.**
- `parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/utils/ManagedPCInstance.java` (modify)
- `parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/utils/KafkaClientUtils.java` (modify)
- `parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/chaostests/ChaosScenarioBase.java` (modify)

**Approach.**
1. `Config` gains an optional output topic and a produce-shaped user function. `run()` supplies a
   producer and calls `pollAndProduce` when the commit mode is transactional, keeping today's
   `poll()` path otherwise. Without a producer, `transactionsValidation()` rejects the mode at
   construction.
2. Create the producer inside `run()`, never once per instance: `run()` is re-entered on every
   RESTART action, and a closed or fenced producer cannot be reused.
3. Thread the transaction-timeout parameter added in U4 through, so KTD4's choice is per-scenario.
4. `Config` gains an optional commit interval. `run()` currently calls
   `setTimeBetweenCommits(Duration.ofSeconds(1))` unconditionally, which would both prevent the
   sub-second interval R13 requires and overwrite the 100ms transactional auto-reduction that C5
   proves. Apply it only when the Config sets it, leaving the transactional default otherwise.
5. `ChaosScenarioBase#newInstance` and `#conductorFor` carry the new callback shape.

**Approach note.** `ChaosConductor` needs no change - it only starts, drains and closes instances,
and `ProducerManager#close` already aborts an open transaction under the commit lock, which is the
abort path a transactional chaos run wants exercised.

**Test scenarios.**
- A `Config` with transactional mode and no output topic fails fast with a clear message rather than
  surfacing as `transactionsValidation`'s generic error.
- A RESTART action produces a working instance - the second `run()` does not reuse the first
  producer.
- A `Config` with no commit interval set leaves the transactional 100ms default in place.
- Non-transactional scenarios are unaffected in behaviour.

**Verification.** `ChaosChurnStormIT`, `ChaosRevokeUnderWorkIT` and `ChaosRevokeUnderWorkCooperativeIT`
still pass unchanged - the cooperative variant extends the same base and builds from the same
`Config`, so it is a third affected scenario, not a variant of the other two.

### U8. Output-side exactly-once ledger

**Goal.** Assert zero duplicates on the output topic, distinct from the input-side ledger.

**Requirements.** R14.

**Files.**
- `parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/chaostests/EosOutputLedger.java` (new)
- `parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/chaostests/EosOutputLedgerIT.java` (new)

**Approach.** A pure function over the observed output multiset and the expected key set, mirroring
`ProgressProbe#ledger` in shape but with a zero duplicate allowance. Its unit test is broker-free
and deliberately untagged so it gates every default integration build, exactly as
`ProgressProbeLedgerIT` does.

**Approach note.** Distinguish "not yet visible because a transaction is unresolved" from "lost".
An abandoned, unfenced transaction pins the LSO for the whole `transaction.timeout.ms` window, and a
ledger that cannot tell those apart will report data loss for a healthy run.

**Test scenarios.**
- Expected set fully present, no duplicates: passes.
- One duplicated value: fails, naming the value and its count.
- One missing value: fails, distinguishing loss from duplication in the message.
- Values absent behind an unresolved transaction are reported as pending, not lost.
- Marker-inflated end offsets do not affect the verdict - the ledger consumes records, never
  offsets.

**Verification.** The ledger's own test fails on a single injected duplicate.

### U9. Commit-timeout failure classification

**Goal.** Make a commit-lock timeout visible to the chaos tripwire, which currently swallows it.

**Requirements.** R15.

**Files.**
- `parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/utils/ManagedPCInstance.java` (modify)
- `parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/chaostests/ChaosScenarioBase.java` (modify)

**Approach.** Per KTD7, the commit-lock timeout arrives as a `java.util.concurrent.TimeoutException`
wrapped as the cause of a `RuntimeException`, and `isExpectedCloseException`'s blanket
`instanceof TimeoutException` arm currently classifies it as an expected close. First narrow that
arm so the commit-lock timeout - identified by type plus the "Timeout getting commit lock" message
from `ProducerManager#acquireCommitLock` - is no longer swallowed. Then give it a dedicated
classification with a counted, explicit SLO, per `docs/plans/2026-08-01-001` §7.4.

**Execution note.** Narrowing the whitelist changes behaviour for the existing scenarios too. Run
W1, W4 and the cooperative variant before and after the narrowing and record that their verdicts are
unchanged; if they are not, that is a finding about the existing suite, not a reason to widen the
arm back.

**Test scenarios.**
- A commit-lock timeout is reported under its own classification with a count, not as an expected
  close and not as an unclassified failure.
- An unrelated `TimeoutException` on a genuine close path is still expected.
- An unrelated `InternalRuntimeException` is still unclassified and still fails the run.
- Exceeding the commit-timeout SLO fails the scenario.

**Verification.** Injecting a commit-lock timeout produces the new classification; the same injection
against the unmodified whitelist is observed to be silently swallowed, which is the negative control
proving the tripwire was blind.

### U10. Chaos scenario W5: EoS under churn

**Goal.** Run transactional mode under the adversarial conditions the mode is hardest for.

**Requirements.** R13, R14, R23.

**Files.**
- `parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/chaostests/ChaosTransactionalEosIT.java` (new)

**Approach.** Follow the W1/W4 scenario skeleton: declare constants, build a `Config`, `bootstrapFleet`,
tune the probe, chain the conductor, `startRun`, await, `settleRun`, `assertScenarioSlos`. Combine
the three levers `docs/inflight/test-chaos-phase2.md` rosters together rather than separately: KEY
ordering to concentrate commit contention per shard, an explicitly named sub-second commit interval,
and transactional mode. Tag `@Tag("chaos")` so it stays out of every gating suite. Own bounds, not
W4's (KTD3).

**Execution note - do not let the known bug be absorbed.** A second-instance-start arm reproduces
the shape of astubbs#44 (confluentinc#803), whose fix is still open as astubbs#29. State W5's
expected verdict per branch: RED on master with cause astubbs#44, GREEN on the astubbs#29 branch.
Derive U9's commit-timeout SLO on the astubbs#29 branch, not on master - an SLO fitted to a master
that still deadlocks would classify the live defect as an in-SLO event and report a green run that
proves nothing.

**Execution note - dwell.** Set the dwell below the transaction timeout chosen in KTD4 for the main
arm, so a fenced producer is a finding rather than the default outcome.

**Test scenarios.**
- Under rebalance and restart churn, the output topic read at `READ_COMMITTED` satisfies the U8
  ledger with zero duplicates.
- Starting a second instance against a running transactional instance does not stall the first;
  the arm's verdict is recorded per branch.
- No instance ends with an unclassified failure cause; commit-lock timeouts appear under the U9
  classification and within its SLO.
- The run's seed and replay command are logged, and replaying the seed reproduces the schedule.
- A deliberately short transaction timeout arm produces aborted transactions whose records never
  appear on the output topic.

**Verification.** Green on an idle box across repeated seeds on the astubbs#29 branch; any RED
replayed on an idle box before being called a product bug, per `docs/plans/2026-08-01-001` §4.

### U11. Calibration arms and negative controls

**Goal.** Make "proved" mean something. This is the evidence unit, not a formality.

**Requirements.** R16, R17, R18.

**Files.** This plan's results section.

**Approach.** For each claim marked `PROVED`, break the mechanism it guards and record that the test
failed for the right reason. Where a real defect exists, prefer it to an injected one. A claim whose
negative control could not be produced is recorded `COVERED_NO_CONTROL`, not `PROVED`.

**Execution note - instrumentation must reach the run.** Use
`./mvnw -pl parallel-consumer-core -am verify` and confirm `BUILD SUCCESS` on the compile step
before trusting any arm. Two traps void an experiment silently: `-pl` without `-am` leaves both arms
running the stale class, and `surefire:test` alone does not reprocess test resources. Better still,
assert the setting in the run's own output - PC logs its full options at INFO on init.

**Execution note - measure with a tool that selects the test (KTD13).** `bin/soak-test.sh` drives
failsafe only. Use it for `*IT` classes; measure unit-lane rates with a direct
`./mvnw -pl parallel-consumer-core -am test -Dtest='<Class#method>'` loop. A soak run reporting
`0/N` on a unit-lane class has run nothing.

**Test scenarios.**
- U3's ordering invariant: move the produce-lock release before the mailbox handoff; expect
  deterministic failure. Same-magnitude-different-position, per `docs/plans/2026-08-03-001` §11.
- U13's exactly-once assertion: RED on master at `batchSize >= 2`, GREEN on the merged produce-lock
  fix. Report both as `n/N`.
- U4's visibility assertion: remove the non-vacuity guard and confirm the test passes against an
  unassigned consumer - demonstrating why the guard exists.
- U8's ledger: one injected duplicate.
- U9's classification: the same commit-lock timeout observed to be silently swallowed by the
  unmodified whitelist.
- U1's register: an edited javadoc sentence fails the drift check.

**Verification.** Every `PROVED` claim has a recorded observed failure of its negative control. Any
claim without one is recorded `COVERED_NO_CONTROL`.

### U12. Docs: results, corrections, issue filing

**Goal.** Land the findings where the next reader will look, and where a user will.

**Requirements.** R3, R19.

**Files.**
- `parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/ParallelConsumerOptions.java` (only for a doc-overreach refutation)
- `src/docs/README_TEMPLATE.adoc` (verified-as-of note; and corrections for doc-overreach)
- `README.adoc` (regenerated, never hand-edited)
- `docs/inflight/` (one file per discovered defect)

**Approach.** Write the results into this plan: claim, verdict, evidence, rate. Triage each
refutation per R3 - correct the documentation only for doc-overreach, and for a code-defect leave the
guarantee standing and file the defect. File an issue and a `docs/inflight/` entry for every
refutation and every defect found (R19), and link the issue from any correction in the fully
qualified `astubbs/parallel-consumer#NN` form. Add a short verified-as-of note to
`[[transaction-system]]` recording which claims are machine-checked and by which test classes, then
regenerate the README - for a fork whose differentiator is that its guarantees are checked, that
note is the only user-visible output when every claim holds.

**Execution note.** Do not add a `CHANGELOG.adoc` entry. Release notes are generated from the commit
log; a PR never adds one.

**Test scenarios.** `Test expectation: none -- documentation only. The regenerated README is verified
by the build's own generation step, and references by bin/check-issue-refs.sh.`

**Verification.** `bin/check-issue-refs.sh` clean; regenerated README diff contains only the
intended changes; every refutation has an issue number recorded.

---

## Verification Contract

| Gate | Command |
|---|---|
| Unit lane | `./mvnw -pl parallel-consumer-core -am verify -DskipITs=true` |
| Integration lane | `bin/ci-integration-test.sh` |
| Chaos lane | `./mvnw -Pci -pl parallel-consumer-core -am verify -DskipUTs=true -Dincluded.groups=chaos -Dexcluded.groups=` |
| Chaos replay | append `-Dchaos.seed=<seed>` |
| Rate measurement, integration | `bin/soak-test.sh '<ITClass#method>' <runs>`, low `SOAK_FREE_CORES` |
| Rate measurement, unit | `./mvnw -pl parallel-consumer-core -am test -Dtest='<Class#method>'` in a loop |
| Copyright headers | runs in `validate`; skip with `-Dcopyright.skip=true` |
| Issue references | `bin/check-issue-refs.sh` |

Lane rules that constrain where new code may go: `TestConventionRules` fails the build for anything
extending `BrokerIntegrationTest` outside an `integrationTest(s)` package, and for a unit test class
surefire's defaults would not collect. Integration classes are selected by package, so `*IT` is the
correct suffix there.

New chaos code must carry `@Tag("chaos")` or it destabilises the gating integration lane - one
shared static broker, failsafe running methods in parallel, `forkCount=1C` under `-Pci`.

Do not add a flaky test to the unit lane. A flaky core test silently disables the whole PIT mutation
lane, and `rerunFailingTestsCount` does not rescue it because PIT reads the raw result. Note also
that astubbs#224 removes CI test retries, so a load-sensitive test has less cover than it did.

No test that the plan predicts fails on master may merge into the gating lane - see KTD11.

---

## Definition of Done

**Global.**
- Every enforced constant in the claim register is referenced by at least one test, and every
  constant's recorded sentence still matches its source - both enforced by U1's coverage test.
- Every claim marked `PROVED` has a negative control that was observed to fail for the right reason.
  A claim without one is recorded `COVERED_NO_CONTROL` and is not counted as proved.
- Every refuted claim is triaged per R3, and every refutation and discovered defect has a filed
  issue and a `docs/inflight/` entry per R19.
- Rates are reported as `n/N` with load conditions, measured with a tool that selects the test.
- The existing chaos scenarios - including the cooperative variant - still pass unchanged.
- No assertion in any existing test was weakened, no timeout widened, no retry added.
- Nothing red merges into the gating lane.
- Abandoned experimental code from arms that did not pan out is removed, not left in the diff.
- The PR body completes `.github/PULL_REQUEST_TEMPLATE.md` honestly, cites references in the fully
  qualified `astubbs/parallel-consumer#NN` form, and adds no `CHANGELOG.adoc` entry.
- The pre-merge defect-class sweep ran and its result is stated, including "none found" with where
  it looked.

**Per phase.**
- Phase A (U1-U6, U13, and U11's Phase A arms, plus U12's results for them) is independently
  shippable and proves or refutes every claim outside chaos.
- Phase B (U7-U10, U11's chaos arm) adds the adversarial run. If it outgrows the PR, it lands
  separately and its claims are recorded `NOT_YET_COVERED` with a reason, which the coverage test
  reports without failing.
