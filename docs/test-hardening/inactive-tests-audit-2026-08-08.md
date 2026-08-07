# Inactive tests audit

**Date:** 2026-08-08
**Scope:** every test on `master` that does not run, does not assert, or was never written.
**Supersedes:** `disabled-and-weakened-tests-audit-2026-04-22.md`, which existed only on the
unmerged branch `refactor/test-hardening`. All of its findings are carried across below, and the
two it got wrong are corrected with commit evidence. Nothing of it remains only on that branch.

This is a point-in-time record, not a generated index. Every number states the command that
reproduces it, so a future reader re-derives a count instead of rebuilding the analysis. Findings
are keyed by class and method, never by line number - the predecessor's line numbers had drifted by
~22 lines within four months, which is what made accurate findings look stale.

---

## The answers

| Question | Answer |
|---|---|
| **How many tests are disabled?** | **5** test methods. Not 7 - see the grep correction below. |
| **Why is each disabled?** | **1 of 5 says.** `ProgressBarTest.width` carries `@Disabled("For reference sanity only")`. The other four record no reason anywhere: no annotation message, no comment, and a disabling commit that never names the test. |
| **How many tests are empty?** | **1** - `SampleTestingFailsafePluginInclusionCore.test`, body `{ }`. |
| **How many are placeholders?** | **4** - one trivially-false stub, one whose name promises what its body never does, one abandoned mid-write, one diagnostic that cannot fail. |
| **Tests you intended to write?** | **10 were deleted rather than written**, by upstream `confluentinc#493`. Neither follow-up that would have restored them (`confluentinc#494`, `confluentinc#496`) ever merged. |

Two further categories nobody asked about, because an annotation grep cannot see them and they are
the ones that actually mislead:

| | |
|---|---|
| **Tests that assert nothing** | **15 of 292** (5.1%). They can fail only by throwing. |
| **Tests that report green with their assertions branched away** | **1 test, 5 sites** - `OffsetEncodingTests`, via a helper named `assumeWorkingCodec` that is not an assumption. |

**Currently quarantined: zero.** `docs/QUARANTINED_TESTS.md` and the code agree.

---

## Reproducing the counts

```bash
# Every test-bearing annotation shape - enumerate rather than assume a whitelist.
# An earlier draft of this audit undercounted by 3 because it hard-coded a list
# that omitted @CartesianTest.
grep -rhoE "^[[:space:]]*@[A-Z][A-Za-z]*Test\b" --include="*.java" \
  $(find . -type d -path '*/src/test*/java') | sort | uniq -c
grep -rc --include="*.java" "^[[:space:]]*@Test\b" \
  $(find . -type d -path '*/src/test*/java') | awk -F: '{s+=$2} END {print "@Test",s}'

# Raw disabled hits (over-counts - see below)
grep -rn "@Disabled\|@Ignore" --include="*.java" .

# Live quarantines (both must be 0)
rg -c --no-filename "^\s*@Quarantined" -g '*.java' . | awk '{s+=$1} END {print s+0}'
grep -c "^- \[ \]" docs/QUARANTINED_TESTS.md
```

Empty-body, placeholder and assertion-free detection needs a brace-matching parser that masks
comments, string literals and text blocks - a regex misclassifies Java around lambdas and nested
braces. Those counts were derived that way and every hit was then read in source.

---

## The denominator

**292 test methods** = 242 `@Test` + 45 `@ParameterizedTest` + 3 `@CartesianTest` + 2 `@RepeatedTest`.
Zero `@TestFactory`, and zero *literal* `@TestTemplate`.

Plus **9 `@ArchTest` fields** across 7 classes. These are real, enforced tests and are invisible to
an annotation grep, so the honest total is **301**.

Two traps worth naming, because a whitelist-based count walks into both:

- **`@CartesianTest`** (junit-pioneer) contributes 3 methods in `TransactionAndCommitModeTest`. It is
  meta-annotated `@TestTemplate`, so "zero `@TestTemplate`" is true only of the literal string. A
  grep returns 6 lines for these 3 methods, because each also carries `@CartesianTest.MethodFactory`.
- **`@ArchTest`** fields are enforced tests that no `@Test` grep sees.

| Module | Source root | Methods |
|---|---|---|
| parallel-consumer-core | `src/test` | 196 (166 + 30) |
| parallel-consumer-core | `src/test-integration` | 65 (51 + 9 + 3 + 2) |
| parallel-consumer-vertx | `src/test` | 10 (8 + 2) |
| parallel-consumer-vertx | `src/test-integration` | 1 |
| parallel-consumer-reactor | `src/test` | 7 (5 + 2) |
| parallel-consumer-mutiny | `src/test` | 7 (5 + 2) |
| examples/example-core | `src/test` | 2 |
| examples/example-{metrics,reactor,streams,vertx} | `src/test` | 1 each |

**Methods are not executions.** 50 of the 292 are parameterised, cartesian or repeated and run N
times each, mostly `@EnumSource` over `CommitMode` / `ProcessingOrder` / `OffsetEncoding` - and the
three `@CartesianTest` methods run the full `CommitMode` x `ProcessingOrder` cross-product.
Separately, `CommitRejectionTestBase` declares one `@Test` and has two concrete subclasses, so that
one declared method runs twice.

Worth noting in passing: **the example modules have almost no tests** - 6 test methods across all of
`parallel-consumer-examples` (8 counting their 2 `@ArchTest` fields), against 258 in core.

---

## 1. Disabled tests

### The grep over-counts

`grep -rn "@Disabled\|@Ignore"` returns **7**. Two are not disabled tests:

- **`Quarantined.java`** - a javadoc mention of `{@code @Disabled}` inside the fork's own
  `@Quarantined` annotation. Not code. It is worth quoting, because it is the policy this whole
  section is measured against:

  > This is a quarantine, not a kill switch: `@Disabled` loses the signal entirely and, as the
  > drain-zombie investigation proved, a "known flake" can be a real product bug.

- **`AbstractQuarantineScriptTest`** - `@DisabledOnOs(OS.WINDOWS)`, a class-level *platform guard*
  on an `abstract` class with **zero test methods of its own**. Its three concrete subclasses hold
  27 test methods between them (`CheckQuarantineOwnersScriptTest` 10, `QuarantineRegistryScriptTest`
  10, `QuarantineLaneReportScriptTest` 7 - anchored `^\s*@Test`; an unanchored grep returns 12 for
  the registry test because two of its hits are a comment and a string fixture, which is the very
  thing that file exists to test). It skips on Windows only, so on every CI runner and dev machine
  this project uses it **skips nothing**.

Zero `@Ignore` (JUnit 4) anywhere in the tree.

A third counting rule applies but has no instance today: a class-level `@Disabled` would disable
every method beneath it, not one. Worth remembering if that ever changes.

**Real disabled test methods: 5.**

### 1.1 `ParallelEoSStreamProcessorTest.offsetsAreNeverCommittedForMessagesStillInFlightLong`

- **Annotation message:** none.
- **Disabled by:** `c1fefbc64`, Antony Stubbs, 2020-08-27, *"Create and commit offset map"*.
- **Reason:** **none recorded.** The commit body never names the test. What can be said from
  evidence: the same diff rewrites commit expectations across neighbouring tests in the file (e.g.
  `assertCommits(of(1), ...)` → `assertCommits(of(0, 2), ...)`), and the commit's own bullets claim
  "Stabilise tests" and "Faster, more reliable tests". Offset coalescing changed what gets committed,
  and this test was parked rather than reworked. **That is inference from the commit's stated goals,
  not a stated reason.**
- **History:** introduced enabled 2020-05-27 alongside its `Simplest` and `Short` siblings. Ran green
  for three months.
- **Confidence:** high on attribution, low-to-medium on reason.
- **Covered elsewhere?** Partly. `...Simplest` and `...Short` are both enabled and cover the same
  invariant at lower volume; `Short` asserts the final coalesced offset. What `Long` uniquely covers
  - six records, deeper in-flight interleaving - is not covered elsewhere.

### 1.2 `ParallelEoSStreamProcessorTest.processInKeyOrder`

- **Annotation message:** none.
- **Disabled by:** `c1fefbc64` - **the same commit as 1.1**. Both disables landed together.
- **Reason:** **none recorded**, same as 1.1. The test asserts exact commit sets, which is precisely
  the class of assertion that commit rewrote everywhere else in the file.
- **History:** introduced enabled by `17196170a` (2020-05-29, *"Ordered parallel message processing
  by key"*), substantially reworked by `565230cd5` (2020-06-04). Ran green for three months.
- **Confidence:** high on attribution, low-to-medium on reason.
- **Covered elsewhere?** **Substantially, yes** - key ordering is not uncovered. Enabled coverage
  includes `WorkManagerTest.orderedByKeyParallel`, `WorkManagerTest.highVolumeKeyOrder`
  (parameterised 1..1000 over 100 keys), `ParallelEoSStreamProcessorTest.processInKeyOrderWorkNotReturnedDoesntBreakCommits`,
  `ParallelEoSStreamProcessorTest.lessKeysThanThreads`, the four `ShardKeyTest` methods, and several
  integration tests running `ordering(KEY)`.

  More runs across `CommitMode` than is obvious: `TransactionAndCommitModeTest`'s three
  `@CartesianTest` methods exercise the **full `CommitMode` x `ProcessingOrder` cross-product,
  including KEY**, end-to-end against a real broker. That is real coverage and this audit should not
  undersell it.

  **What is genuinely lost, after accounting for all of that:** the end-to-end, multi-partition,
  per-`CommitMode` assertion that offset *commits* respect key-order blocking across partitions.
  `WorkManagerTest` covers the shard and dispatch layer, not the commit layer. The cartesian tests
  assert only **counts** (keys consumed, produced-acknowledged, processed == produced), never
  committed offsets, and run on `numPartitions = 1` - the default inherited from
  `BrokerIntegrationTest` and never overridden - so they cannot observe cross-partition blocking at
  all. `MultiInstanceRebalanceTest` spans two partitions but pins one `CommitMode` and asserts
  consumed keys. `MultiInstanceHighVolumeTest` hardcodes both. The closest enabled sibling,
  `processInKeyOrderWorkNotReturnedDoesntBreakCommits`, does inspect commits but runs a single
  `CommitMode`.

  This is the most substantive coverage gap in this audit, and it survived a deliberate attempt to
  find a test that closes it.

### 1.3 `VertxTest.handleHttpResponseCodes`

- **Annotation message:** none.
- **Created and disabled by:** `61f4c0e41`, Antony Stubbs, 2020-05-27, *"Non blocking http IO
  support with Vert.x WebClient"* - the `@Test`, the `@Disabled` and the signature all arrive on
  adjacent added lines.
- **Reason:** **none recorded.** A placeholder written alongside the Vert.x module and never
  implemented. Its entire body is:

  ```java
  assertThat(true).isFalse();
  ```

- **Never ran green.** It was born disabled and, by construction, could never have passed.
- **Confidence:** high.
- **Covered elsewhere?** No. Non-200 handling is untested in the vertx module. The nearest real test
  in the file, `testHttp`, asserts `statusCode()` is 200 on the happy path only.
- This is a **deletion candidate, not a re-enablement candidate** - there is nothing here to restore.

### 1.4 `MultiInstanceRebalanceTest.largeNumberOfInstances`

- **Annotation message:** none; the javadoc carries the only prose.
- **Created and disabled by:** `53052f512`, Antony Stubbs, authored 2022-02-09, *"fix: Concurrency
  and State improvements (#190)"*. Javadoc, `@Disabled`, `@Test` and the whole method arrive in one
  hunk - **born disabled**, never enabled on master.
- **Reason: runtime cost, not failure.** Two independent lines of evidence. Its javadoc, written by
  the same commit, says *"This test takes some time, but seems required in order to expose some race
  conditions without syntehticly creatign them"* - no failure is claimed. And it was authored as part
  of the fix for the very issues it names; a test written to reproduce a bug, in the commit that
  fixes it, was not parked because it was red.
- **The issues it names** (verified upstream): `confluentinc#188` *"ConcurrentModificationException in
  ShardManager during Rebalancing"* and `confluentinc#189` *"NullPointerException in PartitionMonitor
  during Rebalancing"*, both opened 2022-02-10 and closed 2022-02-25.
- **Confidence:** high.
- **Status:** it carries no `@Tag` on master, so it is fully off rather than merely excluded from a
  lane. `docs/plans/2026-07-29-002-fix-drain-zombie-spin-and-uber-experiment-plan.md` records its
  measured behaviour (~90% pass, 10-20% residual stalls) and the run recipe. **PR `astubbs#29` is
  open and already adds the `@Tag("performance")`** that would move it into the performance lane -
  so this one has an owner, and should not be touched here.

### 1.5 `ProgressBarTest.width`

- **Annotation message:** `@Disabled("For reference sanity only")`.
- **Created and disabled by:** `af1fa5de41`, Antony Stubbs, 2020-06-17 - born disabled with its
  reason stated.
- **Reason:** a deliberate manual/visual check. Confirmed on three axes: the explicit message, zero
  assertions in the body, and placement in the `...integrationTests.sanity` package. It renders a
  progress bar with `Thread.sleep(100)` per step so a human can look at it.
- **Confidence:** high. **This is the only one of the five that documents itself.**
- **Nothing to cover** - there is no product behaviour here. Leave it alone.

### Summary of section 1

| Test | Real coverage lost? | Reason recorded? |
|---|---|---|
| `offsetsAreNeverCommitted…Long` | yes, partial | no |
| `processInKeyOrder` | yes, one specific gap | no |
| `handleHttpResponseCodes` | no - never worked | no |
| `largeNumberOfInstances` | yes, but owned by `astubbs#29` | cost, via javadoc |
| `ProgressBarTest.width` | no - not a behaviour test | yes |

**Every `@Disabled` on master predates the `@Quarantined` mechanism that replaced it.** `AGENTS.md`
and the `@Quarantined` javadoc both prohibit this pattern today: a quarantined test is diagnosed,
owned by a fix PR, and still executed in a non-gating lane, so the signal survives. None of the five
above has any of that.

---

## 2. Empty tests

**1.**

`SampleTestingFailsafePluginInclusionCore.test`, in
`parallel-consumer-core/src/test-integration/.../integrationTests/`. The body is `{ }` - no
statements, no comments. The whole file is 13 lines. The name suggests it existed to prove the
failsafe plugin picked up the `test-integration` source root; nothing in the tree references it
today, and the build proves that by itself now.

Zero other empty bodies. Zero entirely-empty or entirely-disabled test classes.

---

## 3. Placeholder and aspirational tests

**4.** Each is a different shape of "not really written".

**3.1 Trivially-false stub - `VertxTest.handleHttpResponseCodes`.** Body is `assertThat(true).isFalse()`,
parked behind `@Disabled` so it never runs. Also section 1.3.

**3.2 Name promises what the body never does - `ParallelEoSStreamProcessorTest.closeWithoutRunningShouldBeEventBasedFast`.**

```java
@ParameterizedTest()
@EnumSource(CommitMode.class)
public void closeWithoutRunningShouldBeEventBasedFast(CommitMode commitMode) {
    setupParallelConsumerInstance(getBaseOptions(commitMode));
    parallelConsumer.closeDontDrainFirst();
}
```

Nothing measures "fast". This is compelling rather than circumstantial, because **the test
immediately above it in the same file does it properly**:

```java
Duration durationOfCloseOperation = time(() -> { parallelConsumer.close(); });
Duration expectedDurationOfClose = JavaUtils.max(timeBetweenCommits, ofSeconds(2));
assertThat(durationOfCloseOperation).as("Should be fast").isLessThan(expectedDurationOfClose);
```

The pattern, the `time()` helper and the idiom were all a few lines above and were not applied. The
only backstop is the class-level `@Timeout(value = 3, unit = MINUTES)`, and three minutes is not a
meaningful bound on "event based fast".

**3.3 Abandoned mid-write - `WorkManagerOffsetMapCodecManagerTest.stringVsByteVsBitSetEncoding`.**
Every line that computed the comparison is commented out (`// int compressedBytes = …`,
`// int compressedBits = …`, `// int rlBytesCompressed = …`) along with the `log.info` that printed
them. What remains computes `inputLength`, `byteByte`, `bitsBytes` and `runlengthBytes` - **all
unused** - and logs the input. It is named for a three-way comparison it no longer performs.

**3.4 Diagnostic masquerading as a test - `JavaEnvTest.checkJavaEnvironment`.** The whole class is
one `log.error("Java all env: {}", …)`. Its own javadoc concedes it: *"Used to manually inspect the
java environment at runtime"*. It cannot fail, and it emits a `log.error` on every CI run.

**Zero** occurrences tree-wide of `fail("not implemented")`, `UnsupportedOperationException`, `NYI`,
or a test body consisting only of a comment. Command:

```bash
grep -rniE "UnsupportedOperationException|not implemented|notImplemented|\bNYI\b" \
  --include="*.java" $(find . -type d -path '*/src/test*/java')
```

---

## 4. Tests intended but never written

**10 were deleted rather than implemented**, and the intent survives only in git history and an
unmerged branch.

Upstream `confluentinc#493` - *"minor: tests: Removes empty/not implemented tests"*, merged 2022-12-07
as `544593edd` - removed ten test methods:

| Test | What it was |
|---|---|
| `ParallelEoSStreamProcessorTest.avro` | Five-line comment spec (*"send three messages - 0,1,2 / finish processing 1 / make sure no offsets are committed / finish 0 / make sure offset 1, not 0 is committed"*) then `assertThat(false).isTrue()` |
| `ParallelEoSStreamProcessorTest.poisonPillGoesToDeadLetterQueue` | dead-letter-queue behaviour |
| `ParallelEoSStreamProcessorTest.userSucceedsButProduceToBrokerFails` | partial-failure path |
| `ParallelEoSStreamProcessorTest.failingMessagesDontBreakCommitOrders` | failure vs commit ordering |
| `ParallelEoSStreamProcessorTest.failingMessagesThatAreRetriedDontBreakProcessingOrders` | retry vs processing ordering |
| `ParallelEoSStreamProcessorTest.messagesCanBeProcessedOptionallyPartitionOffsetOrder` | optional partition-offset ordering |
| `ParallelEoSStreamProcessorTest.ifTooManyMessagesAreInFlightDontPollBrokerForMore` | in-flight backpressure |
| `WorkManagerOffsetMapCodecManagerTest.truncationOnCommit` | `@Disabled("TODO: Blocker: Not implemented yet")` |
| `WorkManagerTest.maxPerPartition` | empty body |
| `WorkManagerTest.maxPerTopic` | empty body |

The six middle rows matter most. Dead-letter queues, retry ordering and in-flight backpressure are
core library behaviours, and the only surviving record that tests for them were *wanted* is this
deletion diff.

**Neither follow-up merged.** `confluentinc#494` (*"Reenables disabled tests, to be fixed"*) and
`confluentinc#496` are both named in the deleting commit's own body. The branch behind the latter,
`origin/refactor/empty-tests` @ `5f8b3dba`, has three commits - the removal, *"START: Put back the
removed empty tests, to be implemented"*, and a third that adds the stubs. At its tip, **only 3 of
the 10** carry `throw new NotImplementedException()` (`avro`, `maxPerPartition`, `maxPerTopic`). Six
return unchanged - still `@Disabled`, still empty or `assertThat(false).isTrue()` - and
`truncationOnCommit` returns *without* its `@Disabled` while keeping `assertThat(true).isFalse()`,
which would have made it a hard red. So even the restoration was partial. No PR for that branch
exists on the fork.

So the honest answer is: the *removal* half landed; the *implementing* half never did, and the
record of what was wanted now lives one `git show` away from being lost.

Two more standing intentions, not deleted but never fulfilled:

- **`MultiTopicTest`** carries a wholesale commented-out assertion helper `assertCommit(...)` with
  the note *"depends on merge of features/consumer-interface branch"*. That branch exists as
  `origin/features/consumer-interface` but was never merged, so the dependency has been unmet for
  years. This is an assertion the suite was meant to make and does not.
- **`LargeVolumeInMemoryTests`** carries `// TODO: Assert process ordering` - a named missing
  assertion inside a test that passes without it.

---

## 5. Tests that do not run, but report as a pass

No annotation grep finds these, and they are the most misleading category in the tree.

**5.1 `assumeWorkingCodec` is not an assumption.** `OffsetEncodingTests` wraps assertions at five
sites in `if (assumeWorkingCodec(encoding, encodingsThatFail)) { … }`. Despite the name, that helper
is simply:

```java
return !encodingsThatFail.contains(encoding);
```

So for `BitSet`, `BitSetCompressed`, `BitSetV2`, `RunLength` and `RunLengthCompressed`,
`ensureEncodingGracefullyWorksWhenOffsetsAreVeryLargeAndNotSequential` **runs to green with most of
its assertions branched around, and reports as a pass rather than a skip.** A test that looks like it
covers all codecs, reports that it covers all codecs, and materially checks about half of them. The
`assume*` name is what conceals it.

**5.2 Assumption aborts - 5 tests, one site each.** All sit inside `@ParameterizedTest` methods, so they
drop parameter *values* rather than whole tests, and the drop is invisible unless you read the report
carefully: `ParallelEoSStreamProcessorTest` (AssertJ `Assumptions.assumeThat`), `WorkManagerTest`
(`assumeFalse(order == KEY)` - drops the KEY ordering case), `RunLengthEncoderTest`,
`CloseAndOpenOffsetTest`, `OffsetEncodingTests` (skips 4 `OffsetEncoding` values on every run).

**5.3 Early-return guard - 1.** `QuarantinedAnnotationContractTest` returns early when
`target/test-classes` is absent. It is commented and benign, and it is the only bare `return;` in any
test body in the tree - but it is a silent pass if the path assumption ever breaks. `assumeTrue(...)`
would make the skip visible in the report.

---

## 6. Tests that assert nothing

**15 of 292 (5.1%).** These can fail only by throwing.

Getting this number right required resolving helper indirection: **46** methods had no assertion
token in their own body, but **31 were cleared** because the helper they call asserts internally.
The repo uses seven coexisting assertion styles - AssertJ, Google Truth, the fork's generated
`ManagedTruth`, JUnit 5 Jupiter, Mockito `verify`, Awaitility and Hamcrest - so a naive grep
misclassifies badly in both directions.

### The 15

| Test | Shape |
|---|---|
| `MutinyTest.emitOnExample`, `.runSubscriptionOnExample` | library demo; subscribes with `System.out::println` |
| `ReactorTest.publishOn`, `.subscribeOn` | same shape |
| `JavaEnvTest.checkJavaEnvironment` | one `log.error` (also 3.4) |
| `ParallelEoSStreamProcessorTest.closeWithoutRunningShouldBeEventBasedFast` | name/body mismatch (also 3.2) |
| `MockConsumerEarlyCloseTest.mockConsumer` | 70-line hang test; the class `@Timeout` is the only check - **and it is broken, see section 8** |
| `ShardKeyTest.nullKey` | throws-only NPE regression guard |
| `InterruptionTests.waitOnZeroCausesInfiniteWait` | `@Timeout(1, SECONDS)` is the check |
| `WorkManagerOffsetMapCodecManagerTest.runLengthEncodingCompression` | helper verified to only log |
| `WorkManagerOffsetMapCodecManagerTest.stringVsByteVsBitSetEncoding` | comparison commented out (also 3.3) |
| `CustomConsumersTest.extendedConsumer` | construction smoke test for `confluentinc#195` |
| `SampleTestingFailsafePluginInclusionCore.test` | empty (also section 2) |
| `ProgressBarTest.width` | visual check (also 1.5) |
| `VertxTest.sanityTest` | `tc.verify(() -> { log.debug(…); tc.completeNow(); })` |

### What cleared the other 31, so the number is auditable

- The nine `CoreBatchTest` / `ReactorBatchTest` / `MutinyBatchTest` methods clear via
  `BatchTestMethods.{averageBatchSizeTest, simpleBatchTest, batchFailureTest}`.
- `VertxBatchTest`'s three clear through **two** hops - `@Test` overload → `@Override` overload →
  `batchTestMethods.*`.
- The six `TransactionMarkersTest` methods clear via `waitForRecordsToBeReceived` → its overload,
  which contains `await().untilAsserted(assertThat…)`.
- `MultiInstanceRebalanceTest`'s three clear via `runTest()` (`assertAll`, `assertThat`,
  `untilAsserted`) - though one of those three is `largeNumberOfInstances`, which is `@Disabled`, so
  it is "cleared" in the sense of having assertions, not of running.
- `TransactionAndCommitModeTest`, `PartitionStateCommittedOffsetIT`, `LoadTest`,
  `VeryLargeMessageVolumeTest`, `RunLengthEncoderTest`, `MultiInstanceMetricsTest`,
  `ChaosRevokeUnderWorkIT` and `ParallelEoSSStreamProcessorRebalancedTest` all clear the same way,
  through a named helper that asserts.

### Judgement calls, stated

- **One mis-detection was caught and corrected.** `VertxTest.sanityTest` initially cleared as
  "Mockito verification". It is not Mockito - it is `VertxTestContext.verify`, which only propagates
  exceptions thrown inside the block, and that block only logs and completes. Anyone re-running this
  analysis must special-case `tc.verify`.
- **Two were counted as checked despite having no `assert*`**, and the reasoning is recorded so it
  can be disputed: `VertxTest.genericVertxFuture` (its `awaitLatch` throws `TimeoutException`, and
  `tc.checkpoint(3)` must be flagged three times) and `LoadTest.asyncConsumeAndProcess`
  (`await().atMost(60s).until(<boolean supplier>)` is a real check).
- **Sanity check performed:** no test was cleared purely by an assertion sitting in dead code - zero
  tests have all their assertions at brace depth >= 3.
- **Residual limitation:** an assertion inside a callback that is registered but never fired at
  runtime would still read as "asserting" here. Only execution tracing could rule that out.

### Which of the 15 matter

Genuinely worth fixing: `closeWithoutRunningShouldBeEventBasedFast`, `stringVsByteVsBitSetEncoding`,
`MockConsumerEarlyCloseTest.mockConsumer` (via the timeout bug in section 8).

Defensible as they stand: the four `MutinyTest`/`ReactorTest` library scratchpads (they catch API
breakage by throwing on upgrade), `ShardKeyTest.nullKey` (a throws-only NPE guard - its javadoc names
the scenario, *"Tests when KEY ordering with `null` keyed records"*, without stating that contract
explicitly),
`InterruptionTests.waitOnZeroCausesInfiniteWait` (the 1-second timeout is a correct deliberate
check), `CustomConsumersTest.extendedConsumer` ("does not throw" genuinely is the assertion here),
and `ProgressBarTest.width` (honestly labelled).

Probably delete: `SampleTestingFailsafePluginInclusionCore`, `JavaEnvTest.checkJavaEnvironment`.

---

## 7. Quarantined tests

**Zero**, and the registry is accurate.

```bash
rg -c --no-filename "^\s*@Quarantined" -g '*.java' . | awk '{s+=$1} END {print s+0}'   # 0
grep -c "^- \[ \]" docs/QUARANTINED_TESTS.md                                           # 0
```

A naive `grep -rn "@Quarantined"` returns 13, but **every one is a string fixture inside the
quarantine script tests, or javadoc** - not a live quarantine. This category is held separate from
section 1 deliberately: a quarantined test is diagnosed, owned by a fix PR, and still executed in a
non-gating lane. A disabled test is none of those things. They are not two flavours of the same
thing.

---

## 8. Carried forward from the 2026-04-22 audit

The predecessor covered ground beyond disabled tests. Those findings are recorded here so its branch
copy holds nothing this document lacks. **They are recorded, not fixed.**

### 8.1 Kneecapped volumes

| Location | Issue | Status |
|---|---|---|
| `LargeVolumeInMemoryTests` | 500 vs 1,000,000 messages | **Open.** Still `int quantityOfMessagesToProduce = 500;` with `// 1_000_000` commented above it, `git blame`d to `565230cd5a` (2020-06-04) and untouched since. The predecessor listed this as fixed by PR #49, but carried a caveat - *"Check whether this fix has been merged to master before flagging again"* - and it had not: PR #49 never touched the file. The restore-to-1M commit exists only on `refactor/test-hardening`. |
| `MultiInstanceHighVolumeTest` | `30_000_00` - an underscore typo giving 3M, not 30M | **Open. A real defect, not a judgement call.** |
| `VeryLargeMessageVolumeTest` | 1M vs 2M | Open - decide and clean up |
| `LoadTest` | 4K vs several commented-out alternatives | Open - dead code |
| `TransactionAndCommitModeTest.numThreads` | 64 vs 1000 | Open - document or fix |
| `TransactionAndCommitModeTest.roundsAllowed` | 10, with a TODO | Open - likely masks a real issue |

### 8.2 Weak and commented-out assertions

- `VertxConcurrencyIT` - `assertNumberOfThreads()` commented out at two sites.
- `ParallelEoSStreamProcessorTest` - two commented `assertCommits` calls; verify the refactor that
  replaced them is equivalent.
- `MultiInstanceHighVolumeTest` - commented-out processing-delay logic.
- `MultiTopicTest` - the commented-out `assertCommit` helper described in section 4. *(New here; the
  predecessor did not have it.)*

### 8.3 Commented-out test methods

**1** - `sanity/StreamTest.test`, where `@Test` is commented out above an intact method body that
would run forever (an infinite `Stream.generate` printer with no terminal limit). The class has a
real test alongside it. Delete candidate.

This count was re-derived independently here, by masking every file and keeping only `@Test`
occurrences whose byte offset falls inside a comment region. Three raw hits; two are prose (in
`TestConventionRules` and `QuarantineRegistryScriptTest`). **It agrees with the predecessor's count
of 1.**

### 8.4 Tag-based exclusions - not a problem

`@Tag("performance")` is excluded from default builds via `excluded.groups` in `pom.xml`, covering
`LargeVolumeInMemoryTests`, `MultiInstanceHighVolumeTest` and `VeryLargeMessageVolumeTest`. They run
in the PR performance suite. This is correct behaviour and is recorded only so nobody re-flags it.

### 8.5 Where the predecessor was wrong

Two of its conclusions are **refuted** by the git history, which it did not consult - it recorded
its reasons as *suspected*, attributed none of them to a commit, and noted an intent to
`git blame` that it never carried out:

| It said | Actually |
|---|---|
| `…Long` was "probably disabled due to flakiness on slow CI" | Disabled by `c1fefbc64`, an offset-map/commit-semantics change. No flakiness evidence exists anywhere. |
| `processInKeyOrder` was "probably flaky or the feature had bugs when it was written" | Disabled by **the same commit**, in the same hunk pattern, on the same day. |

Both refutations rest on the same evidence: `Simplest`, `Short` and `Long` were all introduced
together by `61f4c0e41` on 2020-05-27 and ran green for three months, so nothing about their history
suggests flakiness. It is worth stating plainly that the predecessor did **not** claim `Short`
superseded `Long` - that was a hypothesis raised while investigating, and the history refutes it, but
it was never the predecessor's position.

It was right about `largeNumberOfInstances` being a cost problem, and right to recommend
`@Tag("performance")` over `@Disabled`. And its `LargeVolumeInMemoryTests` entry carried a caveat to
re-check before flagging - see §8.1, where dropping that caveat is exactly how an earlier draft of
*this* document turned an open defect into a closed one.

Its line-number references (347, 574, 199) had drifted to 369, 596 and 206 by the time this audit ran
- four months. That is why this document keys everything by class and method instead.

---

## 9. Surprises

Four things found along the way that nobody was looking for.

**9.1 Three `@Timeout(60000L)` annotations mean 16 hours 40 minutes, not 60 seconds.** JUnit 5's
`@Timeout` defaults to `TimeUnit.SECONDS`, so `@Timeout(60000L)` is 60,000 *seconds*. Affected:
`MockConsumerEarlyCloseTest`, `MockConsumerSaslAuthenticationTest`, `MockConsumerCommitTimeoutTest`.

This is worse than cosmetic because of how it interacts with section 6:
`MockConsumerEarlyCloseTest.mockConsumer` has **no assertion at all** - the entire test is "PC closes
rather than hanging", and the timeout *is* the assertion. With the unit wrong, it cannot fail by
hanging within any realistic CI budget; it would simply wedge the job. Every other `@Timeout` in the
tree is written correctly (`@Timeout(60)`, `@Timeout(120)`, `@Timeout(value = 3, unit = MINUTES)`),
so these three are the outliers, and the `L` suffix is the tell that the author was thinking in
milliseconds.

**Highest value-to-effort fix in this document.**

**9.2 `OffsetEncodingTests` uses JUnit 4's `org.junit.Assume` inside a Jupiter test, and it works
only by accident of the classpath.** No pom declares JUnit 4 (`junit.version` is 5.14.4); it arrives
transitively, from `testcontainers` 1.21.4 declaring `junit:junit` 4.13.2 at compile scope. It
currently behaves because `junit-jupiter-engine` ships
`OpenTest4JAndJUnit4AwareThrowableCollector`, which reflectively recognises
`org.junit.internal.AssumptionViolatedException` *if junit4 is on the classpath*.

If junit4 ever leaves the test classpath, the `import static org.junit.Assume.assumeThat` fails to
**compile** - so this surfaces as a build break, not a silent behaviour change, which is the better
of the two outcomes. Were it to compile, the skip would become a hard failure for four enum values.
Fixing it is a one-line change to `org.junit.jupiter.api.Assumptions`, but note that would not
remove the project's JUnit 4 surface: `CoreAppMetricsIntegrationTest` also imports
`org.junit.Assert`.

**9.3 `assumeWorkingCodec` conceals what it does** - see section 5.1. Of everything in this audit,
this is the finding most likely to mislead someone reading a green test report.

**9.4 Wall-clock burned by a test that cannot fail.** `MockConsumerEarlyCloseTest.mockConsumer`
sleeps 5 seconds unconditionally and asserts nothing - so with 9.1's broken timeout, that is ~5
seconds per run spent on a test with no way to fail except by throwing. (`ProgressBarTest.width`
sleeps 10 seconds by construction but is `@Disabled`, so it costs nothing.)

---

## 10. What to do about it

Nothing in this document changes a single test. These are the follow-ups it justifies, and none of
them is started here.

**Trivial and safe:**

1. `@Timeout(60000L)` → `@Timeout(60)` in the three classes (9.1). Restores a real check to an
   assertion-free test.
2. Delete `VertxTest.handleHttpResponseCodes` - never ran, cannot pass, tests nothing (1.3).
3. Delete `StreamTest.test`'s commented-out method (8.3).
4. Fix the `30_000_00` underscore typo (8.1).

**Honesty fixes:**

5. Rename `assumeWorkingCodec` to `isWorkingCodec`, or convert its call sites to real per-value
   assumptions so the skips appear in the report (5.1).
6. Point `OffsetEncodingTests` at `org.junit.jupiter.api.Assumptions` (9.2).
7. Add the timing assertion to `closeWithoutRunningShouldBeEventBasedFast`, copying the idiom from
   the test just above it (3.2).

**Needs a decision:**

8. `processInKeyOrder` (1.2) - the end-to-end key-order commit assertion is the one real coverage
   gap. Re-enabling means reconciling it with the commit semantics `c1fefbc64` introduced.
9. `offsetsAreNeverCommitted…Long` (1.1) - same commit, same reconciliation.
10. `stringVsByteVsBitSetEncoding` (3.3) and `MultiTopicTest.assertCommit` (8.2) - restore the
    comparison/assertion, or delete it. Leaving it implies coverage that does not exist.
11. `LargeVolumeInMemoryTests` runs 500 messages where 1,000,000 is commented out (8.1). Previously
    believed fixed; it is not. The restore-to-1M work exists on `refactor/test-hardening` along with
    OOM diagnostics from running it - salvage both before that branch is deleted.
12. The remaining kneecapped volumes (8.1) - decide the intended value and record why.

**Already owned - do not touch:**

- `largeNumberOfInstances` (1.4). PR `astubbs#29` carries the `@Tag("performance")` change.

**Deliberately not built:** a generated `docs/INACTIVE_TESTS.md` with a `--check` staleness gate,
modelled on `bin/todo-index.sh`. It is consistent with repo convention and worth revisiting, but it
does not address why the previous audit was lost - that was invisibility, not drift - and its gate
would fail the PR Checklist job on any open PR that touches a test annotation, `astubbs#29`
included.

Per repo convention, triage for these lives in `docs/refactoring.md`, not here. This document is the
inventory and the evidence.
