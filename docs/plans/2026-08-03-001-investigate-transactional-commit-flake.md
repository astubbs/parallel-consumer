# Investigation handoff: `ProducerManagerTest.producedRecordsCantBeInTransactionWithoutItsOffsetDirect`

**Status:** open. Failure mechanism CONFIRMED by local reproduction; the remaining question is which
layer owns the fix. This document is a starting point, not a conclusion.
**Written:** 2026-08-03
**Branch:** `investigate/producer-transaction-commit-flake`
**Branched from:** `932a7032` (`master` — "fix(core): a rebalance-time commit no longer kills the broker-poll thread (#857 family) (#100)")

Start here. Two things are established and two are not — keep them apart:

- **Established (§3):** the failure reproduces locally (1 in 4 runs, no PIT needed) and the mechanism
  is nailed down — the commit takes offsets one behind, *after* both records have been produced.
- **NOT established (§4):** whether the window that allows this exists in **production** or only in
  this test's hand-rolled harness. That is the whole question, and §4 is a lead to verify or destroy,
  not a finding. Say which you did.

---

## 1. What is failing

`ProducerManagerTest.producedRecordsCantBeInTransactionWithoutItsOffsetDirect` fails
non-deterministically. Two distinct impacts:

1. **The required `Unit Tests` gate** — logged since 2026-07-28, ~1 failure in 245 local runs.
2. **The whole PIT mutation lane** — PIT refuses to run when any test is unstable *without* mutation:

   ```
   1 tests did not pass without mutation when calculating line coverage.
   Mutation testing requires a green suite.
   SEVERE : Tests failing without mutation:
   Description [testClass=io.confluent.parallelconsumer.internal.ProducerManagerTest,
                method:producedRecordsCantBeInTransactionWithoutItsOffsetDirect()]
   ```

   So this single race turns `Mutation (PIT, scoped) (optional)` red and **no mutants are scored at
   all**. Observed on PR #108, run `30785700182`, job `91598708692`.

This is the **second** test to disable the PIT lane this way — PR #101 fixed
`ParallelEoSStreamProcessorTest.queuedMessagesNotProcessedOrCommittedIfSubmittedDuringShutdown` for
exactly the same reason. That makes the class of problem worth naming: *any* flaky core test silently
disables mutation testing repo-wide, so a green PIT lane has been evidence of suite stability rather
than of mutation coverage.

### Why `rerunFailingTestsCount` is not the answer

It would hide the flake from the surefire-driven unit gate, but **PIT runs its own coverage pass and
sees the raw result**, so the mutation lane stays red. That buys silence on the required gate while
keeping the outage on the optional one. Fix it, don't retry it.

---

## 2. Why this test matters more than a typical flake

Read the test's name as a specification: **produced records cannot be in a transaction without their
offset.** That is the core EOS invariant of the transactional commit mode. If it can fail, the
possibilities are:

- the test asserts an ordering PC never promised (→ test bug), **or**
- PC really can commit a transaction containing a produced record whose *source* offset is not in the
  same transaction (→ a genuine exactly-once correctness bug).

The second would mean: after a crash, the produced record survives while its input record is
redelivered — a duplicate that EOS is specifically supposed to prevent.

**Do not assume it is the first.** Two bugs in this same family — offsets recorded at a point the
broker never accepted — were just fixed on the *consumer* commit path in #100 and #108, and both
initially looked like flaky tests. This one is on the *producer* path. It may be the third instance,
or it may be nothing. That question is the investigation.

---

## 3. The failing assertion

At the end of the test (`ProducerManagerTest.java`, method
`producedRecordsCantBeInTransactionWithoutItsOffsetDirect`):

```java
final int nextExpectedOffset = 2; // as only first of two work completed
var producer = module.producerWrap();
Mockito.verify(producer, description("Both offsets are represented in base commit"))
        .sendOffsetsToTransaction(
            UniMaps.of(mu.getPartition(), new OffsetAndMetadata(nextExpectedOffset, "")),
            mu.consumerGroupMeta());
```

### CONFIRMED — reproduced locally, 2026-08-03

Reproduced on this branch (base `932a7032`) at **iteration 4 of 25** of a bare
`-Dtest=ProducerManagerTest#producedRecordsCantBeInTransactionWithoutItsOffsetDirect` loop, with no
PIT and no artificial load. It is not rare, and it is not PIT-specific:

```
org.mockito.exceptions.base.MockitoAssertionError:
Both offsets are represented in base commit

Argument(s) are different! Wanted:
producerWrapper.sendOffsetsToTransaction(
    {topic-0 = OffsetAndMetadata{offset=2, leaderEpoch=null, metadata=''}}, ...);

Actual invocations have different arguments at position [0]:
    ...
    producerWrapper.send(Mock for ProducerRecord, hashCode: 99235024, ...)     <- produce #1
    producerWrapper.send(Mock for ProducerRecord, hashCode: 545770597, ...)    <- produce #2
    producerWrapper.flush()
    producerWrapper.flush()
    producerWrapper.beginTransaction()
    producerWrapper.sendOffsetsToTransaction(
        {topic-0 = OffsetAndMetadata{offset=1, leaderEpoch=null, metadata='bgAA'}}, ...);
```

**The offset is one behind, exactly as predicted** — `offset=1` where the test wants `offset=2`.

Two details that matter more than the mismatch itself:

- **Both `send` calls precede the commit.** The transaction therefore contains *two* produced records
  while committing an offset that says the second source record was never consumed. That is the
  literal EOS violation this test's name forbids — not a cosmetic count-off.
- **The metadata is `'bgAA'`, not `''`.** That is PC's encoded incomplete-offset payload: at commit
  time PC knew offset 1 was still outstanding and encoded it as such. So this is not a lost update or
  a torn read — PC coherently committed the state "offset 1 not done" *after* having produced the
  record derived from offset 1.

The mechanism is now settled: **the commit collected offsets while offset 1's work was produced but
not yet recorded complete.** What remains open is only §4 — whether that window exists in production
or only in this test harness.

---

## 4. Leading hypothesis (UNVERIFIED): the test releases the produce lock earlier than production does

### The invariant the main code implements

`ProducerManager.preAcquireOffsetsToCommit()` takes the commit (write) lock **before** offsets are
collected, with this comment in `commitOffsets`:

```java
// producer commit lock should already be acquired at this point, before work was retrieved to commit,
// so that more messages don't sneak into this tx block - the consumer records of which won't yet be
// in this offset collection
```

So the design is: *commit lock held ⇒ nobody is producing ⇒ the offsets collected are consistent with
the records produced.* The produce side takes a **read** lock (`beginProducing`), the commit side a
**write** lock — so a commit cannot start while any produce is in flight.

That invariant only holds if the produce lock is released **after** the work is registered as
complete. Otherwise there is a window: lock released, commit proceeds, but the work has not yet been
counted, so the offset committed is one behind.

### Production ordering — mailbox first, unlock last

`AbstractParallelEoSStreamProcessor.runUserFunction` (~line 1348):

```java
try {
    return runUserFunctionInternal(usersFunction, context, callback, activeWorkContainers);
} finally {
    cleanUpContext(context);          // <- unlocks the ProducingLock, LAST
}
```

and inside `runUserFunctionInternal`:

```java
resultsFromUserFunction = userProcessingTimer.record(() -> usersFunction.apply(context));
for (var kvWorkContainer : activeWorkContainers) onUserFunctionSuccess(...);
...
for (var kvWorkContainer : activeWorkContainers) addToMailBoxOnUserFunctionSuccess(...);  // mailbox
```

Work reaches the mailbox, *then* `cleanUpContext` releases the produce lock. Correct order — no
window.

### Test ordering — unlock inside the user function, before the mailbox

The test supplies a raw `Function` and manages the lock itself:

```java
Function<PollContextInternal<String, String>, List<Object>> userFunc = context -> {
    newValue = producerManager.beginProducing(mock(PollContextInternal.class));
    try {
        ...
        module.producerWrap().send(mock(ProducerRecord.class), (a, b) -> {});
        return UniLists.of();
    } finally {
        newValue.unlock();            // <- INSIDE the user function
    }
};
```

That `unlock()` runs when the user function returns — i.e. **before** the wrapper's
`addToMailBoxOnUserFunctionSuccess`. The window the production path closes is open here.

**The test already says so itself**, in a pre-existing comment on that very `finally`:

```java
// this unlocks the produce lock too early - should be after WC returned. Need a call back? plugin?
// Should refactor the wrapped user function to can construct it?
// also without using wrapped user function- we're not testing something important
```

and above the function: `// todo refactor to use real user function directly`.

### What this predicts

If the hypothesis holds, the failure is a **test-harness artifact**: the control thread occasionally
wins the race to the commit lock in a window that the real wrapped user function never opens. The fix
is then to make the test drive the real wrapped path (its own TODO), not to widen a timeout.

### How to refute it

The hypothesis is wrong — and this is a real bug — if any of these turn out true:

- some production path releases the produce lock before the work reaches the mailbox (check every
  caller of `ProducingLock::unlock` / `finishProducing`, including
  `WorkContainer.java:273` and the error/stale paths `handleStaleWork` and the failure branch that
  calls `addToMailbox(context, wc)` before `finally { cleanUpContext(context); }`);
- `addToMailbox` is asynchronous in a way that leaves the work uncounted after it returns, so even the
  production ordering has the window;
- the captured offset is *not* one behind (then the mechanism is something else entirely).

That third bullet is why §3 comes first.

---

## 5. What has already been tried (do not repeat)

- **Timeouts were already widened for PIT** in commit `9e133ce0` ("test: fix test pollution — leaked
  threads, Awaitility state, race conditions"). The test carries the evidence:

  ```java
  // 20s (was default 10s): tight under PIT's instrumented JVM
  ...
  }, ofSeconds(20)); // was 10s; too tight under PIT
  ```

  **The flake survived that.** So "it's just a slow CI box" has been tested and is not sufficient as
  an explanation. Widening them again is not a fix.
- The class carries `@Timeout(60)`.

## 6. Why PIT provokes it

PIT's coverage pass runs instrumented and single-minion: 362 seconds to compute coverage on the #108
run, slowest single test 135s. That stretches every interval in the test and widens whatever window
the race needs — which makes PIT a *useful reproducer*, not merely a victim.

---

## 7. Suggested approach

1. ~~Get the real assertion failure.~~ **Done — see §3.** The offset is one behind (`1` vs `2`), both
   produces precede the commit, and the metadata shows PC knew offset 1 was incomplete. Reproduces in
   ~1 in 4 runs locally with no PIT, so you have a fast feedback loop: loop the scoped `-Dtest` command
   and you should see red within a couple of minutes.
2. **Reproduce deliberately** rather than by repetition, once you know the window: insert a delay
   between the user function returning and `addToMailbox`, and see whether the failure becomes
   deterministic. If it does, the window is characterised.
3. **Decide the layer, and say which and why:**
   - test-side → make the test use the real wrapped user function (its own TODO), so lock release and
     mailbox registration are ordered as in production;
   - main-code → then it is an EOS correctness bug, and the test is right to fail. Fix
     `ProducerManager` / the lock lifecycle, keep the test as the regression guard, and it needs a
     `Fixes` changelog entry and probably an upstream issue.
4. **Do not** reach for `rerunFailingTestsCount` or another timeout bump (see §1, §5).

## 8. Verifying a fix

- The scoped run: `./mvnw -o -pl parallel-consumer-core -Dlicense.skip -Dtest=ProducerManagerTest surefire:test`
- Loop it enough times to beat the observed rate. Note the base rate is **much worse than the ~1/245
  in the original ledger entry** — it reproduced 1 in 4 on an idle machine here (see §3), so a few
  dozen green runs is meaningful, but still pair it with the deterministic reproducer from step 2.
- **Confirm the PIT lane recovers**, since that is the visible symptom:
  `bin/ci-mutation-test.sh`. A fix that leaves PIT red has not finished.
- Full suite before commit: `bin/ci-unit-test.sh`.

## 9. Files

| Path | Why |
|---|---|
| `parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/internal/ProducerManagerTest.java` | the failing test; the TODO comments in `producedRecordsCantBeInTransactionWithoutItsOffsetDirect` are the lead |
| `parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/internal/ProducerManager.java` | `preAcquireOffsetsToCommit`, `commitOffsets`, `acquireProduceLock`, `ProducingLock` |
| `parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/internal/AbstractParallelEoSStreamProcessor.java` | `runUserFunction` / `runUserFunctionInternal` / `cleanUpContext` — the production ordering (~lines 1340–1425) |
| `parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/state/WorkContainer.java:273` | the other `finishProducing` caller |
| `docs/inflight.md` | the ledger entry for this flake — **update it as you go** |

## 10. Context worth having

- **`docs/inflight.md` is the repo's ledger** for parked/in-flight work. Record findings there, not in
  a scratch file.
- **The #100 / #108 pattern.** Both were "an offset recorded as committed when it was not", on the
  consumer path, and both were mis-framed at first (#100 as CI load, #108 as an already-handled
  exception). The house lesson from that workstream: when a test fails under stress, establish
  *contention vs genuine bug* before touching the test. A weakened assertion here would hide an EOS
  violation.
- **Don't weaken assertions.** If an exception or interaction is unexpected, classify it — never
  broaden the matcher to make it pass.
- **PIT is currently a canary.** Until this is fixed, the mutation lane reports suite stability, not
  mutation coverage. Worth restating in any PR that touches it.
