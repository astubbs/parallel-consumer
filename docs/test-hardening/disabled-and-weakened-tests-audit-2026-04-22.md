# Disabled, Weakened, and Excluded Tests Audit

**Date:** 2026-04-22
**Scope:** Whole codebase audit at commit on `origin/master` as of 2026-04-22
**Purpose:** Identify tests that have been disabled, commented out, kneecapped (reduced in volume/coverage), or had their assertions weakened. Provide enough context for a future agent to triage and re-enable them where appropriate.

This document is a **reference for an agent** that will later go through each finding, decide whether it's safe to restore, fix the underlying issue if any, and re-enable the test with appropriate coverage. Each finding records what was observed, what the suspected reason is, and the recommended triage approach.

## How to Use This Document

Each finding has:
- **Location** (file path and line number, repo-relative)
- **Current state** (what the code actually does right now)
- **Intent signal** (commented-out code, javadoc, or surrounding context that hints at what it *should* do)
- **Suspected reason for suppression** (flake, performance, bug, refactor leftover)
- **Recommended action** (re-enable, delete, move, investigate)
- **Verification** (how to know the change is safe once applied)

Findings are ordered by confidence and impact. Tackle the **High confidence** section first.

---

## 1. Disabled Tests (`@Disabled`)

### 1.1 `offsetsAreNeverCommittedForMessagesStillInFlightLong` (HIGH)

**Location:** `parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/ParallelEoSStreamProcessorTest.java:347`

```java
@Disabled
@ParameterizedTest()
@EnumSource(CommitMode.class)
void offsetsAreNeverCommittedForMessagesStillInFlightLong(CommitMode commitMode) {
    // ... full test body exists, real assertions ...
}
```

**Current state:** Skipped across all CommitModes. No comment explains why.

**Intent signal:** Full test body is present and validates a critical invariant - "offsets are never committed for messages still in flight." This is a load-style variant of `offsetsAreNeverCommittedForMessagesStillInFlightShort` (which appears immediately above at line ~300 and is enabled).

**Suspected reason:** Timing-sensitive; probably disabled due to flakiness on slow CI. Not a reference test - it tests real behavior.

**Recommended action:**
1. Re-enable and run locally with `rerunFailingTestsCount=2`
2. If flaky, find the timing dependency (likely an `await()` with too-tight timeout) rather than weakening the assertion
3. If stable, remove `@Disabled`

**Verification:** Test should pass on all CommitModes 5 runs in a row locally.

---

### 1.2 `processInKeyOrder` (HIGH)

**Location:** `parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/ParallelEoSStreamProcessorTest.java:574`

```java
@ParameterizedTest()
@EnumSource(CommitMode.class)
@SneakyThrows
@Disabled
public void processInKeyOrder(CommitMode commitMode) {
    // ... full test body with 9 latches testing KEY ordering ...
}
```

**Current state:** Skipped across all CommitModes. No comment explains why.

**Intent signal:** Tests the KEY ordering constraint - one of PC's three core guarantees (UNORDERED, PARTITION, KEY). Without this test, KEY ordering has reduced coverage.

**Suspected reason:** Complex test with 9 latches; probably flaky or the feature had bugs when it was written.

**Recommended action:**
1. Git blame line 574 to find when `@Disabled` was added and whether there's a referenced bug
2. Re-enable and investigate any failures - this is documenting a core contract
3. If the feature is broken, fix it (this is arguably more important than fixing the test)

**Verification:** Test passes on all CommitModes. Confirm KEY ordering guarantee holds under contention.

---

### 1.3 `handleHttpResponseCodes` - stub test (HIGH - trivial delete)

**Location:** `parallel-consumer-vertx/src/test/java/io/confluent/parallelconsumer/vertx/VertxTest.java:199`

```java
@Test
@Disabled
void handleHttpResponseCodes() {
    assertThat(true).isFalse();
}
```

**Current state:** Disabled stub. Body is a placeholder that always fails.

**Intent signal:** Test name suggests intent to verify HTTP status code handling in the Vert.x module.

**Suspected reason:** Abandoned stub.

**Recommended action:** **Delete the method entirely.** If the functionality needs testing, write a new test with a real implementation. Do not re-enable the placeholder.

**Verification:** File compiles; no other tests reference this method.

---

### 1.4 `largeNumberOfInstances` (MEDIUM)

**Location:** `parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/MultiInstanceRebalanceTest.java:95`

```java
/**
 * Tests with very large numbers of parallel consumer instances to try to reproduce state and concurrency issues
 * (#188, #189).
 * <p>
 * This test takes some time, but seems required in order to expose some race conditions without syntehticly
 * creatign them.
 */
@Disabled
@Test
void largeNumberOfInstances() {
    numPartitions = 80;
    int numberOfPcsToRun = 12;
    int expectedMessageCount = 500000;
    // ...
}
```

**Current state:** Disabled. References upstream issues #188 and #189.

**Intent signal:** Javadoc explicitly says "seems required in order to expose some race conditions." This is high-value testing that was disabled for resource/time reasons.

**Suspected reason:** 12 instances + 80 partitions + 500k messages is too heavy for normal CI.

**Recommended action:**
1. Add `@Tag("performance")` instead of `@Disabled`
2. Remove `@Disabled`
3. The test now runs in the performance suite (self-hosted runner) where heavy tests belong

**Verification:** Test appears in performance suite output; stable across 3 runs.

---

### 1.5 `ProgressBarTest.width` (LOW - leave alone)

**Location:** `parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/sanity/ProgressBarTest.java:22`

```java
@Test
@Disabled("For reference sanity only")
public void width() { ... }
```

**Recommended action:** **Leave disabled.** Explicit annotation reason makes the intent clear - this is documentation, not a test.

---

## 2. Kneecapped Volumes

These tests have commented-out large values with smaller substitutes. The commented-out values represent the original intent.

### 2.1 `LargeVolumeInMemoryTests` - 500 vs 1,000,000 messages (FIXED)

**Location:** `parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/LargeVolumeInMemoryTests.java:62`

```java
//        int quantityOfMessagesToProduce = 1_000_000;
        int quantityOfMessagesToProduce = 500;
```

**Status:** Already fixed on branch `refactor/ci-unified-build` (PR #49). The test now runs at 1M as originally intended.

**Note for the agent:** Check whether this fix has been merged to master before flagging again.

---

### 2.2 `MultiInstanceHighVolumeTest` - underscore typo (HIGH)

**Location:** `parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/MultiInstanceHighVolumeTest.java:60`

```java
//        int expectedMessageCount = 10_000_000;
        int expectedMessageCount = 30_000_00;
```

**Current state:** `30_000_00` = **3,000,000** (three million). Underscores in wrong positions.

**Intent signal:** Commented-out value is `10_000_000` (ten million). The current value has misplaced underscores that look like 30 million but evaluate to 3 million. This looks like a typo from someone trying to write `30_000_000` during a reduction.

**Suspected reason:** Accidental typo during a kneecapping pass. Intent was probably 30M (reduction from 10M was unintentional) or 3M (the actual current value).

**Recommended action:**
1. Decide the correct value (probably `3_000_000` with proper underscores, matching current behavior)
2. Remove the commented-out `10_000_000` line
3. Fix the underscore formatting: `int expectedMessageCount = 3_000_000;`
4. Consider whether 3M is enough to exercise multi-instance behavior meaningfully

**Verification:** Test still passes at whatever final value is chosen; runtime is documented.

---

### 2.3 `VeryLargeMessageVolumeTest` - 1M vs 2M (MEDIUM)

**Location:** `parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/VeryLargeMessageVolumeTest.java:90`

```java
//        int expectedMessageCount = 2_000_000;
        long expectedMessageCount = 1_000_000;
```

**Current state:** 1M messages, down from commented-out 2M.

**Recommended action:**
1. Measure current runtime at 1M on CI
2. If there's budget, restore to 2M
3. Otherwise leave at 1M but delete the dead comment

**Verification:** Test remains stable at chosen volume.

---

### 2.4 `LoadTest` - 4K vs many other values (MEDIUM)

**Location:** `parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/LoadTest.java:45-49`

```java
//    static int total = 8_000_0;
//    static int total = 4_000_00;
//    static int total = 4_000_0;
    static int total = 4_000;
//    static int total = 8;
```

**Current state:** 4,000 messages. 4 alternative commented-out values (80K, 400K, 40K, 8).

**Intent signal:** The stack of commented-out alternatives looks like trial-and-error during debugging. No comment explains the chosen value.

**Recommended action:**
1. Delete the dead comment lines (keep the chosen value)
2. Decide if 4K is actually load-testing anything; rename or reconsider if not
3. This is a `DbTest` - the DB connection may be the bottleneck, not the volume

**Verification:** Single uncommented value; test passes.

---

### 2.5 `TransactionAndCommitModeTest.numThreads` - 64 vs 1000 (MEDIUM)

**Location:** `parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/TransactionAndCommitModeTest.java:165`

```java
        // increased PC concurrency - improves test stability and performance.
        int numThreads = 64;
//        int numThreads = 1000;
```

And line 171-173:
```java
//                .numberOfThreads(1000)
//                .numberOfThreads(100)
//                .numberOfThreads(2)
                .maxConcurrency(numThreads)
```

**Current state:** 64 threads. Multiple commented alternatives.

**Intent signal:** Comment says "increased PC concurrency - improves test stability." Unclear whether 64 was chosen because 1000 was unstable or because 64 is enough to exercise the behavior.

**Recommended action:**
1. Clean up commented alternatives
2. Document why 64 is the right number (or change it)
3. If 1000 was unstable, the instability is the bug - investigate

**Verification:** Test passes with chosen value 5 runs in a row.

---

### 2.6 `TransactionAndCommitModeTest.roundsAllowed` - 10 vs 200 (MEDIUM)

**Location:** `parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/TransactionAndCommitModeTest.java:210-217`

```java
        // todo rounds should be 1? progress should always be made
        int roundsAllowed = 10;
//        roundsAllowed = 200;
//        if (commitMode.equals(CONSUMER_SYNC)) {
//            roundsAllowed = 3; // sync consumer commits can take time // fails
////            roundsAllowed = 5; // sync consumer commits can take time // fails
////            roundsAllowed = 10; // sync consumer commits can take time // works with no logging
//        }
```

**Current state:** `roundsAllowed = 10` for all modes. Comment says "todo rounds should be 1? progress should always be made."

**Intent signal:** The TODO explicitly says rounds should be 1 - meaning progress should always be made in a single iteration. The 10 is a workaround for apparent test flakiness/slow progress.

**Suspected reason:** Real bug or timing issue with CONSUMER_SYNC mode masked by allowing more rounds.

**Recommended action:**
1. Investigate why progress sometimes needs 10 rounds
2. Either fix the underlying issue (preferred - the TODO confirms the ideal is 1) or document why 10 is acceptable
3. Clean up the commented-out alternatives

**Verification:** Understand why the current value is what it is; test stability improves or the underlying bug is filed.

---

## 3. Weak / Commented Assertions

### 3.1 `VertxConcurrencyIT` - `assertNumberOfThreads` disabled (MEDIUM)

**Location:** `parallel-consumer-vertx/src/test-integration/java/io/confluent/parallelconsumer/vertx/integrationTests/VertxConcurrencyIT.java:230, 250`

```java
//        assertNumberOfThreads();
```

**Current state:** Two calls commented out at critical points - after all requests received, and after all responses received.

**Intent signal:** Method `assertNumberOfThreads()` is defined and likely checks thread count bounds. The calls are at logical checkpoints where you'd want to verify Vert.x isn't spawning unbounded threads.

**Recommended action:**
1. Check whether `assertNumberOfThreads()` is still implemented and functional
2. If yes, re-enable and see what happens
3. If the assertion is too strict, make it a loose bounds check rather than an exact count
4. If the assertion is obsolete, delete the method and the commented calls

**Verification:** Either the assertions pass, or the method is removed.

---

### 3.2 `ParallelEoSStreamProcessorTest` - commented `assertCommits` calls (LOW)

**Location:** `parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/ParallelEoSStreamProcessorTest.java:477, 485`

```java
//        assertCommits(of(2));
// ... replaced with ...
        assertCommitLists(...);
```

**Current state:** Old assertion method `assertCommits` commented out, replaced with `assertCommitLists`.

**Intent signal:** Looks like a refactor from older assertion API to newer one.

**Recommended action:**
1. Verify `assertCommitLists` is a superset of `assertCommits` (covers the same properties)
2. If yes, delete the commented-out lines
3. If no, restore the missing coverage

**Verification:** Grep confirms `assertCommits` is either equivalent or its behavior is covered by `assertCommitLists`.

---

### 3.3 `MultiInstanceHighVolumeTest` - commented delay logic (LOW)

**Location:** `parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/MultiInstanceHighVolumeTest.java:139-147`

```java
//        int chance = 10;
//        int dice = RandomUtils.nextInt(0, chance);
//        if (dice == 0) {
//            Thread.sleep(100);
//        } else {
//            Thread.sleep(RandomUtils.nextInt(3, 20));
//        }
```

**Current state:** Random per-message processing delay commented out. Processing is now instant.

**Intent signal:** The commented code simulates realistic variable processing time (3-20ms usually, 100ms 10% of the time).

**Recommended action:**
1. Decide whether realistic delays are needed to exercise back-pressure and commit coalescing
2. Either restore as an option (flag-driven) or delete the dead code
3. Without delays, the test may not exercise back-pressure at all - possible coverage gap

**Verification:** Test either has documented realistic timing or the dead code is removed.

---

## 4. Commented-Out Test Methods

### 4.1 `StreamTest.test` - commented `@Test` (LOW - delete)

**Location:** `parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/sanity/StreamTest.java:25`

```java
//    @Test
    public void test() {
        Stream<Double> s = Stream.generate(() -> Math.random());
        s.forEach(x -> {
            log.info(x.toString());
        });
    }
```

**Current state:** Method exists but `@Test` is commented out, so it never runs. Body would run forever (infinite stream with no terminal limit).

**Intent signal:** File is "Sanity test usage of Java Stream" - this is exploratory/reference code, not a real test.

**Recommended action:** **Delete the method entirely.** The class has a real test (`testStreamSpliterators`) that works.

**Verification:** `StreamTest` still has at least one real test method.

---

## 5. Tag-Based Exclusions (Not Bugs - Just FYI)

### 5.1 `@Tag("performance")` default exclusion

**Configured in:** `pom.xml` (surefire/failsafe `excludedGroups` property)

```xml
<excluded.groups>performance</excluded.groups>
```

**Tagged tests:**
- `LargeVolumeInMemoryTests`
- `MultiInstanceHighVolumeTest`
- `VeryLargeMessageVolumeTest`

**Current state:** These are excluded from default builds but included in the PR performance suite job (`bin/performance-test.sh`). This is correct behavior - not a problem.

---

## Summary Table

| # | Location | Issue | Priority | Action |
|---|----------|-------|----------|--------|
| 1.1 | `ParallelEoSStreamProcessorTest.java:347` | `@Disabled` critical offset commit test | HIGH | Re-enable, fix flake |
| 1.2 | `ParallelEoSStreamProcessorTest.java:574` | `@Disabled` KEY ordering test | HIGH | Re-enable, investigate |
| 1.3 | `VertxTest.java:199` | Disabled stub with `assertThat(true).isFalse()` | HIGH | Delete |
| 1.4 | `MultiInstanceRebalanceTest.java:95` | `@Disabled` rebalance stress test | MED | `@Tag("performance")` instead |
| 1.5 | `ProgressBarTest.java:22` | Disabled reference test | LOW | Leave alone |
| 2.1 | `LargeVolumeInMemoryTests.java:62` | 500 vs 1M messages | DONE | Fixed in PR #49 |
| 2.2 | `MultiInstanceHighVolumeTest.java:60` | `30_000_00` typo (3M vs 30M) | HIGH | Fix typo, decide intent |
| 2.3 | `VeryLargeMessageVolumeTest.java:90` | 1M vs 2M | MED | Decide and clean up |
| 2.4 | `LoadTest.java:45-49` | 4K vs many commented alternatives | MED | Clean up dead code |
| 2.5 | `TransactionAndCommitModeTest.java:165` | 64 vs 1000 threads | MED | Document or fix |
| 2.6 | `TransactionAndCommitModeTest.java:210` | roundsAllowed = 10 with TODO | MED | Fix underlying issue |
| 3.1 | `VertxConcurrencyIT.java:230,250` | `assertNumberOfThreads()` commented | MED | Re-enable or delete |
| 3.2 | `ParallelEoSStreamProcessorTest.java:477,485` | `assertCommits` commented | LOW | Verify refactor equivalence |
| 3.3 | `MultiInstanceHighVolumeTest.java:139-147` | Processing delay commented | LOW | Decide and clean up |
| 4.1 | `StreamTest.java:25` | Commented `@Test` on reference code | LOW | Delete |

## Recommended Execution Order

1. **Trivial deletions first** (1.3, 4.1): Drop abandoned stubs and exploratory code. Zero risk.
2. **Fix obvious bugs** (2.2): Fix the `30_000_00` underscore typo.
3. **Promote to performance suite** (1.4): Move `largeNumberOfInstances` from `@Disabled` to `@Tag("performance")`.
4. **Investigate high-value disabled tests** (1.1, 1.2): These test core behavior. Re-enable and fix the underlying flakes or bugs.
5. **Clean up dead comments** (2.3, 2.4, 3.2, 3.3): Decide intent and remove dead alternatives.
6. **Investigate kneecapped workarounds** (2.5, 2.6, 3.1): These likely mask real bugs. Fix the bugs rather than accepting the workarounds.

Each finding should land as its own small PR so regressions can be isolated.
