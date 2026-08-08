---
title: Inactive Test Remediation - Plan
type: test
date: 2026-08-08
artifact_contract: ce-unified-plan/v1
artifact_readiness: implementation-ready
product_contract_source: ce-plan-bootstrap
execution: code
origin: docs/test-hardening/inactive-tests-audit-2026-08-08.md
---

# Inactive Test Remediation - Plan

## Goal Capsule

**Objective.** Act on the inactive-test audit: restore the two dark core tests, make the
silently-passing ones honest, delete what is dead, write the one missing test, and convert the
items that are not tests at all into the right artefact.

**Authority hierarchy.** `AGENTS.md` > this plan > implementer judgement. Where they disagree,
`AGENTS.md` wins and the conflict is reported.

**Stop conditions.** Stop and report rather than proceed when: a re-enabled test fails in a way
the measured tables in this plan do not predict (that is new evidence, not a stale expectation);
any change would require weakening an assertion or loosening a timeout to go green; or an item
turns out to be owned by an open PR that this plan did not account for.

**Execution profile.** Test-code changes plus documentation. One new test. No production-code
changes except a stale javadoc correction.

**Tail ownership.** The caller owns commit, push, PR and CI.

---

## Product Contract

### Summary

The audit found 22 actionable items. Research collapsed most of them: nine of the ten
"never-written" tests should not be written, two of the twelve remediation items are already owned
by open PRs, and the two long-disabled core tests need assertion surgery rather than investigation.
This plan does the remaining real work and records the rest accurately.

### Problem Frame

The audit answered *what is inactive*. Acting on it needs a second question answered: *what is
actually still true*.

Three research passes changed the picture materially. The two tests disabled since 2020 are not a
mystery and not a bug - they fail 100% deterministically, twelve executions out of twelve, and the
library behaviour they exist to protect is intact when measured directly. Nine of the ten deleted
stubs turn out to be already-covered, obsolete, or waiting on features the library never had. And
the audit's own "highest value-to-effort fix" is already implemented, better, in an open PR.

The audit also carried several claims forward from its 2026-04-22 predecessor without re-verifying
them, and four of those are wrong. That is the same failure the audit criticised its predecessor
for, so correcting them is part of this work rather than a footnote.

### Requirements

**Restore the dark tests**

R1. Re-enable `offsetsAreNeverCommittedForMessagesStillInFlightLong` with its assertions retargeted
to measured behaviour, and with the `UNORDERED` ordering its siblings declare - today it would
silently inherit the `KEY` default and exercise two in-flight records instead of six.

R2. Re-enable `processInKeyOrder` with a mode-agnostic assertion layer, so it runs on all three
commit modes rather than only the transactional one.

R3. Neither re-enablement weakens an assertion. Where a stale assertion is replaced, the
replacement asserts at least as much, and the offset-map state is asserted directly where the old
test could only assert its absence.

**Make silently-passing tests honest**

R4. `OffsetEncodingTests` states what it checks. The five codecs it currently branches around are
asserted to degrade in a defined way rather than skipped, and the helper stops being named as if it
were an assumption.

R5. Remove the vacuous assertion in `OffsetEncodingTests` that can never fail.

R6. Remove JUnit 4 from the test compile path in both places it survives, which also unblocks the
JUnit 6 bump.

**Delete what is dead**

R7. Delete the four tests the audit identified as dead, and anything they solely orphan.

R8. Delete dead commented-out alternatives without changing any live value.

**Write what is missing**

R9. Write the one deleted stub whose behaviour exists, is reachable, and is uncovered.

**Record what is not a test**

R10. Record the five deleted stubs that are already covered, naming the covering test for each, so
nobody writes a duplicate.

R11. Convert the three stubs whose feature never existed into issue references rather than test
work.

R12. Record `truncationOnCommit` as obsolete and correct the production javadoc that still
describes the truncation it was named for.

R13. Salvage the `LargeVolumeInMemoryTests` OOM diagnostics from the branch that holds the only
copy, and scope the 1M restore as its own work rather than a cleanup item.

**Keep the record honest**

R14. Correct every audit claim that research refuted, including the ones the audit inherited from
its predecessor.

R15. Upgrade the audit's account of *why* the two core tests were disabled from inference to the
contemporaneous evidence that exists.

### Scope Boundaries

**In scope.** Test code, one new test, the audit's corrections, and the backlog and issue entries
that replace test work which cannot be done.

**Deferred to follow-up work.**

- The three `@Timeout(60000L)` annotations. Open PR `astubbs#206` already replaces them with
  `@Timeout(120)` on a shared base class, and 120 is correct where this plan's originally-intended
  60 would have raced two tests' internal Awaitility budgets of 45s and 50s.
- Moving `largeNumberOfInstances` to `@Tag("performance")`. Owned by open PR `astubbs#29`.
- Restoring `LargeVolumeInMemoryTests` to 1,000,000 messages. Blocked on a measured
  `OutOfMemoryError`, not a value choice - see R13 and U9.
- Building a dead-letter queue, per-partition limits, or per-topic limits. These are features.
- Raising any live volume value. `LoadTest` in particular is untagged, runs in the gating lane, and
  is already a listed member of the load-tightness flake family at its current value.

**Outside this work.** Any change that makes a test pass by weakening it. The `AGENTS.md` rule
applies at full strength, and this plan's evidence exists precisely so that no assertion has to be
softened to get green.

### Sources

- `docs/test-hardening/inactive-tests-audit-2026-08-08.md` - the origin document.
- `origin/bugs/turn-on-commit-tests` @ `009bb7122` - an abandoned single-commit attempt to re-enable exactly these two tests, whose message names the cause.
- `origin/refactor/test-hardening` @ `7e864ed9`, `16ce9727` - the 1M restore and its OOM diagnostics, which exist nowhere else.
- `docs/inflight/test-load-tightness-flakes.md` - the classify-before-touching rule and `LoadTest`'s membership.
- `docs/solutions/test-flakiness/vacuous-await-condition-brokerpoller-backpressure-2026-07-31.md` - the precedent for the vacuous-assertion class in R5.
- Open PRs `astubbs#206` (timeouts), `astubbs#29` (largeNumberOfInstances), `astubbs#38` (JUnit 6, blocked), `astubbs#106` / `astubbs#207` (offsets main code).
- Open PR `astubbs#260` (*"tolerate PC's repeat commit of the same base offset"*) - **a prerequisite**, see Assumptions.
- Open PRs `astubbs#261` (a terminally failed send leaves a partial result set visible under transactions) and `astubbs#257` (stop transactional batches reprocessing records that succeeded) - both own produce-failure and redelivery semantics that U8 asserts.
- Open PR `astubbs#226` - also edits `ParallelEoSStreamProcessorTest`, widening the rebase surface.
- Fork issues `astubbs#149` (DLQ), `astubbs#160`, `astubbs#236` (adjacent limit requests).

---

## Planning Contract

### Key Technical Decisions

KTD1. **Re-enable the two dark tests by rewriting their assertions, not by quarantining them.**
Both fail deterministically - twelve executions, zero variance - and direct measurement of the
commit sequence shows the library behaving correctly at every checkpoint. `@Quarantined` exists for
a test whose *product* is suspect with a fix in flight; here the product is right and the test is
stale. Quarantining would park a known-wrong expectation in a non-gating lane indefinitely.

KTD2. **Assert the degraded contract in `OffsetEncodingTests` rather than converting to
assumptions.** Converting looks more honest and is strictly worse: the five branched-around codecs
are currently checked by the test's own switch statement, and assumptions would discard those
negative assertions in exchange for a skip line in the report. The codecs are not broken - they are
v1 encoders hitting a `short` ceiling against a 72,770-offset input, which is what the test's name
("gracefully works") is about.

KTD3. **Drop the `@Timeout` item entirely rather than racing `astubbs#206`.** It deletes the same
three annotations across the same files and replaces them with an `@Inherited` base-class value.
Doing it here is a hard textual conflict with a PR that already solved it better.

KTD4. **Three deleted stubs become issue references, not tests.** A dead-letter queue,
per-partition limits and per-topic limits do not exist in the library and never have - zero
occurrences in any `src/main/java`. A test cannot be written for absent behaviour, and pretending
otherwise produces a unit the implementer cannot execute.

KTD5. **Change no live volume value; delete only dead commented alternatives.** Every volume the
audit flagged is either already correct, a deliberate increase misread as a reduction, or gated by
a hard-coded wait that would have to move with it. `LoadTest` is the sharp case: untagged, gating,
and already flaking at 4,000, so uncommenting its `4_000_00` alternative would be a hundredfold
increase on a known-fragile test.

KTD6. **The two re-enablements land as separate commits, `Long` first.** They share a root cause
but not a risk profile: `Long` is a bounded ~30-line assertion swap, while `processInKeyOrder`
rewrites the assertion layer of a 150-line nine-latch test across two partitions. Landing them
together means a bisect cannot separate them.

KTD7. **Correct the audit in the same PR as the work.** The audit is the origin document for every
unit here; shipping the work against a document known to be wrong in eight places would leave the
next reader trusting refuted claims.

### High-Level Technical Design

Unit dependencies and the two external PRs that constrain sequencing:

```mermaid
flowchart TB
    U1[U1. Correct the audit]

    subgraph Zero-risk
      U2[U2. Delete four dead tests]
      U3[U3. Delete dead comments]
    end

    subgraph OffsetEncodingTests
      U4[U4. Remove JUnit 4]
      U5[U5. Assert codec degradation]
    end

    subgraph Dark tests
      U6[U6. Re-enable ...Long]
      U7[U7. Re-enable processInKeyOrder]
    end

    U8[U8. Write userSucceedsButProduceToBrokerFails]
    U9[U9. Record non-tests, salvage OOM diagnostics]

    U1 --> U9
    U4 --> U5
    U6 --> U7
    U3 -.must re-run bin/todo-index.sh.-> U3

    PR38[astubbs#38 JUnit 6<br/>blocked] -.unblocked by.- U4
    PR206[astubbs#206 timeouts<br/>open] -.owns the @Timeout item.- U1
    PR29[astubbs#29 largeNumberOfInstances<br/>open] -.owns the @Tag item.- U1
    PR106[astubbs#106 + astubbs#207<br/>own offsets main code] -.may rebase.- U5
```

U2, U3, U4 and U8 are independent and can proceed in any order. U6 must precede U7 - they touch the
same file and share the assertion idiom, and `Long` is the cheaper place to establish it.

**Measured behaviour the re-enablements target.** These tables come from running the choreographies
directly against current `master`, identical across all three commit modes. They record what the
library does, so the implementer is not re-deriving it under time pressure.

**A repeated value in these tables is context, not an assertion target.** Where the same offset
appears twice in a row, that is PC re-committing the same base offset, and how many times it does so
depends on where the wall-clock commit ticks fall - `astubbs#260` collapses exactly these repeats
inside `assertCommits` for that reason. Assert the *set* per partition, never the repeat count.

`offsetsAreNeverCommittedForMessagesStillInFlightLong`, partition 0, `UNORDERED`:

| Step | Committed | Note |
|---|---|---|
| all six in flight | (none) | |
| release 1 | 0 | base offset only |
| release 2 | 0, 0 | repeat - do not assert the count |
| release 0 | 0, 0, 3 | commit advances once 0-2 are contiguous |
| release 3 | 0, 0, 3, 4 | |
| release 4 and 5 | 0, 0, 3, 4, 6 | |

Genesis-trimmed, the final assertion is `of(3, 4, 6)` where the test currently expects `of(2, 3, 5)`.

`processInKeyOrder`, two partitions - partition 1's base offset is 4, because record creation uses a
global offset counter:

| Step | p0 | p1 | Note |
|---|---|---|---|
| A: 6, 8, 1 released; 0 blocking | (none) | 4 | p1 bootstrap at its base offset |
| B: 2 released | 0 | 4 | |
| C: 0 released | 0, 3 | 4 | |
| D: 3, 5 released | 0, 3, 4 | 4, 4 | p1 repeat - do not assert the count |
| E: 4 released | 0, 3, 4 | 4, 4, 7 | the invariant worth guarding |
| F: 7 released | 0, 3, 4 | 4, 4, 7, 9 | |

Read through `assertCommitLists`, which trims partition 0's genesis commit unconditionally and
collects each partition into a set, the assertable per-partition expectations are:
`(p0=[], p1=[4])`, `(p0=[], p1=[4])`, `(p0=[3], p1=[4])`, `(p0=[3,4], p1=[4])`,
`(p0=[3,4], p1=[4,7])`, `(p0=[3,4], p1=[4,7,9])`.

Step E is the invariant worth guarding: partition 1 will not advance past 4 while `key-2`'s offset 4
is in flight, even though 5, 6 and 8 have completed - and it then jumps to 7, not 9, because 8 is
done but not contiguous.

### Assumptions

- **`astubbs#260` lands before U6, U7 and U8.** It rewrites both
  `AbstractParallelEoSStreamProcessorTestBase` and `ParallelEoSStreamProcessorTest` - every file those
  three units touch - and it collapses PC's repeat commit of the same base offset inside
  `assertCommits`, on the stated grounds that how many repeats occur depends on where the wall-clock
  commit ticks fall and so is not something a test can assert on. Starting U6 before it lands means a
  guaranteed conflict in both files and assertions written against a repeat count that is about to
  become uncountable. If it will not land first, stop and re-plan those three units rather than
  racing it.
- The audit branch `chore/audit-inactive-tests` lands before or with this work; this plan's U1 edits
  the audit document, so the two are sequential rather than parallel.
- Re-running a re-enabled test to confirm it is green counts as verification; the forced-execution
  recipe in the Verification Contract is only needed while the `@Disabled` annotation is still
  present.

---

## Implementation Units

### U1. Correct the audit against research

**Goal.** The origin document stops asserting things research refuted, and states the disabling
cause with the evidence that exists.

**Requirements.** R14, R15.

**Dependencies.** None.

**Files.**
- `docs/test-hardening/inactive-tests-audit-2026-08-08.md` (modify)
- `docs/refactoring.md` (modify)

**Approach.**

1. Record that `origin/bugs/turn-on-commit-tests` @ `009bb7122` exists: one commit, deleting exactly
   the two `@Disabled` lines, message *"Turn back on offset commit tests which were dibbled when the
   offset map feature was added"*. Upgrade §1.1 and §1.2 from "no reason recorded / low-to-medium
   confidence" to a named contemporaneous cause. The annotations still carry no message, so the
   "4 of 5 undocumented" headline stands - what changes is that the cause is now evidenced.
2. Correct the `@Timeout` item: it is owned by open `astubbs#206`, which uses `@Timeout(120)`.
   Record that 60 would have raced two internal Awaitility budgets.
3. Correct `30_000_00`: history shows a reduction from `10_000_000`, so the intended value is
   3,000,000. Remove the "may have been 30M" reading.
4. Correct `VeryLargeMessageVolumeTest`: never kneecapped. It went `1_000_000` to `100_0000` - the
   same number - to `1_000_000`. The `2_000_000` was always an aspirational comment.
5. Correct `roundsAllowed`: dead code, assigned and never read, with `ProgressTracker` constructed
   with null rounds. It masks nothing.
6. Correct §8.4: `LoadTest` carries no tag and runs in the gating integration lane;
   `TransactionAndCommitModeTest` is `@Tag("transactions")`, not `performance`.
7. Rewrite §4 with the four-way classification from R10-R12, replacing the current framing that
   presents all ten as lost intent.
8. Correct §5.1: only 3 of the 12 `OffsetEncoding` values run the full positive assertion path, and
   record the second vacuous assertion found at the `doesNotContain(2500L)` line.
9. Note in §1.2 that `TransactionAndCommitModeTest` produces 30,000 unique keys, so under `KEY`
   ordering there is one record per shard and no key contention - its KEY coverage is thinner than
   the audit currently implies.
10. Update the matching entries in `docs/refactoring.md` so the backlog and the audit agree.

**Patterns to follow.** The audit's existing per-finding shape, and its established habit of stating
the reproducing command beside each number.

**Test scenarios.** `Test expectation: none -- documentation. The claims corrected here are verified
by the units that act on them.`

**Verification.** No claim in the audit contradicts the measured tables in this plan. Every
correction names its evidence.

---

### U2. Delete the four dead tests

**Goal.** Remove tests that cannot fail, cannot pass, or never ran.

**Requirements.** R7.

**Dependencies.** None.

**Files.**
- `parallel-consumer-vertx/src/test/java/io/confluent/parallelconsumer/vertx/VertxTest.java` (modify)
- `parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/sanity/StreamTest.java` (modify)
- `parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/SampleTestingFailsafePluginInclusionCore.java` (delete)
- `parallel-consumer-core/src/test/java/io/confluent/csid/utils/JavaEnvTest.java` (delete)
- `parallel-consumer-core/src/test/java/io/confluent/csid/utils/StringTestUtils.java` (delete)

**Approach.**

1. `VertxTest.handleHttpResponseCodes` - delete the method and the now-unused `@Disabled` import.
   `assertThat` stays; it is used at sixteen other sites.
2. `StreamTest.test` - delete the commented-out `@Test` and its method body. The class keeps
   `testStreamSpliterators`. No import becomes unused.
3. `SampleTestingFailsafePluginInclusionCore` - delete the class. Deleting it cannot disable the
   `test-integration` source set: the source root is added declaratively in the root `pom.xml` via
   `build-helper-maven-plugin`, failsafe selects by package pattern rather than source root, and
   twenty-eight other classes in the same package prove inclusion.
4. `JavaEnvTest` - delete the class, not just the method; it has only one. It is also the sole caller
   of `StringTestUtils.pretty()`, which is that class's only method, so delete `StringTestUtils` too
   rather than orphaning it.

**Patterns to follow.** `TestConventionRules` enforces naming for classes that have test methods;
removing a class does not violate it.

**Test scenarios.**

- The vertx module compiles and its remaining tests pass.
- `StreamTest` still collects at least one test method.
- The integration suite still selects and runs `integrationTests` classes after the sample class is
  gone - confirm by observing the failsafe-selected test count is unchanged apart from the deletion.
- No compile error from an orphaned import or an orphaned utility.

**Verification.** Unit and integration suites select and pass the same set of tests, minus exactly
the four removed.

---

### U3. Delete dead commented alternatives, and fix the underscore grouping

**Goal.** Remove dead code that misrepresents current behaviour, without changing any live value.

**Requirements.** R8.

**Dependencies.** None.

**Files.**
- `parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/MultiInstanceHighVolumeTest.java` (modify)
- `parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/VeryLargeMessageVolumeTest.java` (modify)
- `parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/LoadTest.java` (modify)
- `parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/TransactionAndCommitModeTest.java` (modify)
- `docs/TODO_INDEX.md` (regenerate)
- `docs/refactoring.md` (modify)

**Approach.**

1. `MultiInstanceHighVolumeTest`: rewrite `30_000_00` as `3_000_000`. This is byte-identical at
   runtime - it is a readability fix, not a value change. Drop the superseded
   `// int expectedMessageCount = 10_000_000;` line.
2. `VeryLargeMessageVolumeTest`: delete the stale `// 2_000_000` comment. The live value has never
   moved and there is nothing to restore.
3. `LoadTest`: delete the four dead commented alternatives. Leave `total = 4_000` untouched, per
   KTD5.
4. `TransactionAndCommitModeTest`: delete the dead `numThreads` alternatives and the unused
   `roundsAllowed` variable with its commented experiments. Add one line recording why 64 is the
   live value - the adjacent comment already says it was an increase for stability.
5. Move the `// todo rounds should be 1? progress should always be made` question into
   `docs/refactoring.md` before deleting the marker. It is a live unanswered design question
   attached to a variable that stopped being wired in; deleting the variable silently would discard
   the only record of it.
6. Re-run `bin/todo-index.sh` in the same commit. Two of the markers touched here are indexed, and
   `--check` gates the PR Checklist job.

**Patterns to follow.** `docs/TODO_INDEX.md`'s own header records why the index is generated and why
it must be regenerated with any marker change.

**Test scenarios.**

- `bin/todo-index.sh --check` passes after the change.
- No live constant differs from its pre-change value - confirm by diffing the compiled expectations
  or by inspection of each changed line.
- The integration suite behaves identically; these are comment and formatting changes only.

**Verification.** The marker index is current, and the only semantic difference in the diff is
deleted comments.

---

### U4. Remove JUnit 4 from the test compile path

**Goal.** Both surviving JUnit 4 usages move to the assertion library the rest of the repo uses,
which also unblocks the JUnit 6 bump.

**Requirements.** R6.

**Dependencies.** None.

**Files.**
- `parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/offsets/OffsetEncodingTests.java` (modify)
- `parallel-consumer-examples/parallel-consumer-example-metrics/src/test/java/io/confluent/parallelconsumer/examples/metrics/integrationTests/CoreAppMetricsIntegrationTest.java` (modify)

**Approach.**

1. `OffsetEncodingTests`: replace the JUnit 4 `Assume.assumeThat` call with the AssertJ form,
   matching the AssertJ `assertThat` already used throughout the file. AssertJ's assumption throws
   an engine-native abort, so no classpath question arises. Remove the now-unused Hamcrest imports.
2. `CoreAppMetricsIntegrationTest`: replace both `org.junit.Assert` calls with AssertJ. Fix the
   reversed argument order while doing so - the current `assertEquals(actual, expected)` is
   backwards for JUnit 4's signature, so any failure message would have been inverted.
3. Record in the commit message that this removes the last JUnit 4 usage from the compile path, and
   is a prerequisite unblocker for `astubbs#38` rather than a conflict with it.

**Technical design.** Directional only: the assumption's meaning is "skip this parameter when the
encoding is one of four inapplicable values", expressed as a boolean assumption over set membership
rather than a Hamcrest matcher.

**Patterns to follow.** `ParallelEoSStreamProcessorTest` already uses AssertJ's `Assumptions` in
this repo - mirror that call shape.

**Test scenarios.**

- `OffsetEncodingTests` reports the same four skipped parameter values before and after.
- `OffsetEncodingTests` reports the same passed count before and after.
- `CoreAppMetricsIntegrationTest` still passes, and a deliberately broken expectation produces a
  message naming actual and expected the right way round.
- No test source imports `org.junit.Assume` or `org.junit.Assert` after the change. Hamcrest is out
  of scope: it arrives via Awaitility rather than JUnit 4, and six integration tests still use it.

**Verification.** Identical skip and pass counts, and no JUnit 4 import remains in any test source.

---

### U5. Assert what `OffsetEncodingTests` actually checks

**Goal.** The five codecs currently branched around are asserted to degrade in a defined way, and
the helper stops claiming to be an assumption.

**Requirements.** R4, R5.

**Dependencies.** U4.

**Files.**
- `parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/offsets/OffsetEncodingTests.java` (modify)

**Approach.**

1. Rename `assumeWorkingCodec` to `isWorkingCodec`. It is a boolean predicate, not an assumption,
   and the name is what conceals the branching.
2. Add `else` branches at the five call sites asserting the degraded contract, so those codecs move
   from unchecked to checked. The specific assertions depend on what degradation actually looks like;
   derive them by observing the committed metadata before writing them rather than guessing. Sample
   **one codec from each of the two degradation classes named in step 5** - a v1 short-overflow codec
   and the metadata-size codec. They diverge at the first call site, where the working branch asserts
   the metadata is non-blank, so an assertion derived from one class alone will red on the other and
   look like a library finding when it is a sampling error.
3. Remove the duplication between the hard-coded failing-codec list and the switch statement that
   asserts the same set. One of them owns the knowledge; the other reads it.
4. Delete the vacuous `doesNotContain(2500L)` assertion - `2500` is never a record offset in this
   test, so it passes for working and failing codecs alike. The adjacent
   `doesNotContainSequence(expected)` is the assertion doing the work.
5. Record in a comment why the five codecs degrade: v1 `BitSet` and `RunLength` encode length as a
   `short` and overflow past 32,767 against this test's 72,770-offset input, and uncompressed
   `BitSetV2` exceeds the default max metadata size. That reasoning currently exists only in the
   switch statement's own comments.

**Execution note.** Add the `else` assertions before deciding they are correct: run them, observe
what the degraded path actually produces, then commit the assertion that matches. A red here is a
finding about the fallback path, not a stale expectation.

**Patterns to follow.** `docs/solutions/test-flakiness/vacuous-await-condition-brokerpoller-backpressure-2026-07-31.md`
is the precedent for the defect class being removed in step 4.

**Test scenarios.**

- Each of the five previously-branched-around codecs now runs an assertion rather than nothing.
- A working codec still passes the full positive path unchanged.
- The failing-codec set is declared in exactly one place; changing it changes both the branch and
  the switch.
- Removing the vacuous assertion does not change the pass or fail count of any parameter.
- The test still passes for all twelve `OffsetEncoding` values, with four skipped by the U4
  assumption.

**Verification.** Twelve parameters, four skipped, eight asserted - where before only three ran the
full positive path.

---

### U6. Re-enable `offsetsAreNeverCommittedForMessagesStillInFlightLong`

**Goal.** The test runs on all three commit modes and asserts the measured behaviour.

**Requirements.** R1, R3.

**Dependencies.** None.

**Files.**
- `parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/ParallelEoSStreamProcessorTest.java` (modify)

**Approach.**

1. Remove `@Disabled`.
2. Set `ordering(UNORDERED)` explicitly and add the sanity assertion its enabled siblings carry.
   Without this the test inherits today's `KEY` default and exercises two in-flight records instead
   of six - which would silently discard the only coverage this test uniquely provides.
3. Replace every `verify(producerSpy, ...)` call and every `consumerGroupOffsetsHistory()` block
   with the mode-agnostic base-class primitives. Those assertions are producer-transactional only
   and are unreachable by construction for the two consumer-commit modes, which is why two of the
   three parameters fail with "zero interactions with this mock".
4. Retarget the final assertion to `of(3, 4, 6)` per the measured table.
5. Strengthen the intermediate "nothing committed yet" steps: assert the encoded incomplete-offset
   map directly rather than only asserting the absence of a commit. `CommitHistorySubject` already
   provides this and it is the one place the rewrite adds coverage rather than translating it.

**Execution note.** Run the test before changing any assertion, using the forced-execution recipe,
so the starting failure is observed rather than assumed. Every expected offset in this test is stale
by exactly one, because the committed offset became next-expected rather than last-completed.

**Patterns to follow.** The enabled siblings `...Simplest` and `...Short` in the same file - both
declare their ordering and both use the mode-agnostic assertion helpers.

**Test scenarios.**

- Passes for all three `CommitMode` values.
- While offset 0 is in flight, the committed offset does not advance past the partition base, even
  though offsets 1 and 2 have completed.
- Once offset 0 completes, the commit jumps to cover the contiguous run 0-2 in one step.
- The encoded incomplete-offset map names the still-incomplete offsets at each blocked checkpoint.
- Six records are in flight, not two - assert the ordering that makes that true.

**Verification.** Green on all three modes across repeated runs, with the in-flight count asserted
rather than assumed.

---

### U7. Re-enable `processInKeyOrder`

**Goal.** The end-to-end, multi-partition, per-`CommitMode` assertion that committed offsets respect
key-order blocking is running again.

**Requirements.** R2, R3.

**Dependencies.** U6.

**Files.**
- `parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/ParallelEoSStreamProcessorTest.java` (modify)
- `parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/AbstractParallelEoSStreamProcessorTestBase.java` (modify, if the shared helper is added)
- `docs/TODO_INDEX.md` (regenerate - step 3 deletes an indexed marker)

**Approach.**

1. Remove `@Disabled`.
2. Replace the five `awaitForCommitExact(partition, offset)` calls. That helper is implemented via
   producer-transactional verification, so it cannot work for the two consumer-commit modes. Either
   use the existing `await().untilAsserted(...)` plus per-partition commit-list assertion idiom, or
   add a mode-agnostic per-partition await to the base class - the latter is the DRY choice, since a
   second test in the same file already needs it.
3. Delete the `verify(producerSpy, ...never()).commitTransaction()` check. Partition 1's bootstrap
   commit at its base offset makes it false in transactional mode and vacuous in the other two. The
   test's own inline marker already asks for this removal.
4. Retarget every checkpoint using the per-partition expectations listed under the measured table,
   not the raw table rows. The two differ: `assertCommitLists` trims partition 0's genesis commit
   unconditionally - there is no opt-out, unlike `assertCommits` - and collects each partition into a
   set, so repeats collapse and order is not asserted. Partition 1's bootstrap commit at offset 4 is
   *not* trimmed, because the trim only strips a literal 0, so it must be expected at every
   checkpoint.
5. Fix the latent off-by-one at the processed-state seeding: it seeds indices for eight offsets while
   the test has nine records, so the ninth would throw on unboxing if it had not been processed.
6. Leave the processing-state assertions unchanged - they were verified correct against current
   behaviour.
7. Re-run `bin/todo-index.sh` in the same commit. The marker deleted in step 3 is indexed, and
   `--check` gates the PR Checklist job. KTD6 puts this unit in its own commit, so U3's regenerate
   does not cover it.

**Execution note.** This is the unit most likely to go wrong. Change one checkpoint at a time and
re-run, rather than retargeting all five and debugging the result. If a checkpoint disagrees with
the measured table, stop - that is new evidence about the library, not a stale number.

**Patterns to follow.** `offsetCommitsAreIsolatedPerPartition` in the same file already uses the
mode-agnostic per-partition assertion idiom. Merged PR `astubbs#101` is the method precedent for
replacing control-loop-cycle waits with commit-event waits in this file.

**Test scenarios.**

- Passes for all three `CommitMode` values.
- Partition 1 does not advance past its base offset while that offset's record is in flight, even
  though three later offsets on the same partition have completed.
- When the blocking record completes, partition 1 advances to the next incomplete offset rather than
  to the end - a completed but non-contiguous offset is not committed past.
- A record sharing a key with an in-flight record is not processed while that record is in flight.
- A record on the same partition with a different key is processed while another key is blocked.
- Partition 0 and partition 1 commit independently.
- The ninth record's processed state is readable rather than null.
- `bin/todo-index.sh --check` passes after the commit.

**Verification.** Green on all three modes across repeated runs. Confirm the two key-order
invariants are genuinely asserted and would fail if the blocking behaviour regressed - by
temporarily inverting an expectation and observing red, not by inspection.

---

### U8. Write `userSucceedsButProduceToBrokerFails`

**Goal.** Cover the produce-failure path that exists, is reachable, and has no test.

**Requirements.** R9.

**Dependencies.** None.

**Files.**
- `parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/ParallelEoSStreamProcessorTest.java` (modify)

**Approach.**

1. Place it beside the existing happy-path produce test in the same class.
2. Inject a send failure through the existing `producerSpy` - the base class already exposes a
   Mockito spy over a `MockProducer`, so no new helper is needed. Use the repo's existing
   classified test exception rather than a bare `RuntimeException`.
3. Assert the consequence, not just the throw: the record's offset is not committed, and the record
   is retried.
4. Parameterise over `CommitMode` to match its neighbours.
5. Do **not** add a producer-callback-thread case. That asymmetry is owned by open `astubbs#261`,
   which changes whether `produceMessages` throws from the callback under transactions - and it is
   unreachable here anyway, because the base class forces every test onto one auto-completing
   `MockProducer` that invokes callbacks on the calling thread and has no I/O thread to raise from.
6. Note that the redelivery assertion sits on semantics `astubbs#257` is currently changing for
   transactional batches. If the transactional parameter disagrees with the other two, check that PR
   before treating it as a finding.

**Execution note.** Write the failing assertion first and observe it fail for the right reason -
the produce failure, not a setup error - before making it pass.

**Patterns to follow.** `closePCWhenInvalidPidMappingException` in the same class is the closest
existing shape; it covers the one special-cased produce exception, where this covers the general
path. Reuse `assertCommits` / `assertCommitLists` and the generated `ManagedTruth` assertions per
the `AGENTS.md` rule against new helpers.

**Test scenarios.**

- The user function runs and succeeds; the subsequent produce fails.
- The record's offset is not committed.
- The record is redelivered after the retry delay.
- Passes for all three `CommitMode` values.
- The failure is classified rather than swallowed - the test fails loudly if the exception is
  silently dropped.

**Verification.** New test passes on all three modes, and fails if the not-committed assertion is
inverted.

---

### U9. Record the non-tests, and salvage the OOM diagnostics

**Goal.** Every audit item that is not test work becomes the right artefact, and the only copy of
the 1M diagnostics stops living on a branch marked for deletion.

**Requirements.** R10, R11, R12, R13.

**Dependencies.** U1.

**Files.**
- `docs/test-hardening/inactive-tests-audit-2026-08-08.md` (modify)
- `docs/test-hardening/large-volume-in-memory-tests-oom-diagnostics-2026-04-22.md` (create, salvaged)
- `docs/refactoring.md` (modify)
- `parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/state/PartitionStateManager.java` (modify)

**Approach.**

1. Record the five already-covered stubs with the test that covers each, so the coverage claim is
   checkable rather than asserted.
2. Link the three feature-absent stubs to their tracking issues: the dead-letter queue to
   `astubbs#149`, and the per-partition and per-topic limits to the adjacent `astubbs#160` and
   `astubbs#236`. Note that neither adjacent issue is literally a per-scope in-flight cap - if the
   debt should be visible, that is one new issue, not two tests.
3. Record the detail that makes the limit stubs legible: they were written as a trio with
   `maxOverall`, and only the global scope was ever built. It is `WorkManagerTest.maxInFlight` today.
4. Record `truncationOnCommit` as obsolete, and correct the production javadoc on
   `PartitionStateManager.onOffsetCommitSuccess`, which still describes discarding tracked offsets
   below the committed offset. No such truncation happens - the method sets the last committed offset
   and marks the partition clean, and nothing below the committed offset is tracked to discard. The
   comment also references a field that no longer exists.
5. Salvage the OOM diagnostics document from `origin/refactor/test-hardening` into
   `docs/test-hardening/`. It is the only record that 1M exhausts the heap in the close path and
   needs an estimated 2-4GB, and its branch is on the safe-to-delete list.
6. Add a `docs/refactoring.md` entry scoping the 1M restore as real work: the retained
   producer history has to stop being retained, the latch timeout has to move, and the heap has to be
   sized - not a one-line value change.

**Patterns to follow.** The audit's convention of naming the covering test for any "covered
elsewhere" claim.

**Test scenarios.** `Test expectation: none -- documentation, plus a javadoc-only production change.
The javadoc correction alters no behaviour; the existing suite is the regression check.`

**Verification.** No stub in the audit's §4 lacks a disposition. The salvaged diagnostics exist on
this branch. `PartitionStateManager`'s javadoc describes what the method does.

---

## Verification Contract

| Gate | Command | Applies to |
|---|---|---|
| Unit suite | `bin/ci-unit-test.sh` | U2, U4, U5, U6, U7, U8 |
| Targeted unit run | `./mvnw -pl parallel-consumer-core -am test -Dtest='<Class>#<method>'` | U5, U6, U7, U8 |
| Forced run while still disabled | add `-Djunit.jupiter.conditions.deactivate='org.junit.*DisabledCondition'` to the targeted run | U6, U7 baseline only |
| Repeat reproducer | `bin/soak-test.sh '<Class>#<method>' <runs>` | U6, U7 |
| Integration suite | `bin/ci-integration-test.sh` | U2, U3, U4 |
| Marker index | `bin/todo-index.sh --check` | U3, and any unit touching a TODO marker |
| Copyright headers | `bin/check-copyright-headers.sh` | U2, U8, U9 |
| Quarantine registry | `bin/check-quarantine-registry.sh` | all - confirms nothing was quarantined instead of fixed |

**`-am` is not optional.** `AGENTS.md` records that `./mvnw -pl <module>` without it fails the
reactor-convergence enforcer, so the test never recompiles and the run silently executes the previous
build's classes. Every iterative "change one assertion, re-run, observe" loop in U5, U6 and U7
depends on this being right.

Repeat the three-mode runs for U6 and U7 rather than trusting a single green - the point of those
units is that the tests run on all three commit modes, and a single run does not demonstrate it.

No re-enabled or new test may be made green by weakening an assertion, loosening a timeout, adding a
retry, or serialising it. If one of them cannot be made green honestly, stop and report - and note
that `@Quarantined` exists for exactly that case, with a diagnosis and an owning fix PR.

---

## Definition of Done

**Global.**

- Both previously-disabled core tests run on all three commit modes and pass.
- No assertion anywhere was weakened, and no timeout loosened, to achieve green.
- `OffsetEncodingTests` asserts something for every codec it previously branched around, and its
  vacuous assertion is gone.
- No JUnit 4 import remains in any test source.
- The four dead tests are gone, along with anything they solely orphaned.
- No live volume or concurrency constant changed value.
- `bin/todo-index.sh --check` passes.
- Every one of the ten deleted stubs has a recorded disposition: written, covered by a named test,
  tracked as a missing feature, or obsolete.
- The OOM diagnostics exist somewhere other than a branch marked for deletion.
- The audit contains no claim contradicted by the measured tables in this plan.
- Items owned by `astubbs#206` and `astubbs#29` were not touched.
- Any experimental probe or scratch code produced during the work is removed.

**Per unit.** U1: every corrected claim names its evidence. U2: the suites select the same tests
minus exactly four. U3: the only semantic diff is deleted comments. U4: skip and pass counts
unchanged. U5: eight codecs asserted where three were. U6 and U7: green on three modes, repeated.
U8: fails when its central assertion is inverted. U9: no stub without a disposition.
