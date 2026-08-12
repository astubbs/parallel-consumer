# Investigation handoff: `ProducerManagerTest.producedRecordsCantBeInTransactionWithoutItsOffsetDirect`

**Status:** RESOLVED — root cause confirmed by controlled experiment, fixed test-side, guard added.
See §11 for the resolution. Sections 1-10 are the investigation as it stood, kept because the
reasoning is the useful part.
**Written:** 2026-08-03
**Branch:** `investigate/producer-transaction-commit-flake`
**Branched from:** `932a7032` (`master` — "fix(core): a rebalance-time commit no longer kills the broker-poll thread (confluentinc#857 family) (astubbs#100)")

**Resolved — read §11 first.** The §4 hypothesis was confirmed by a controlled experiment: the window
exists **only in the test harness**, not in production. This is a test bug, not an EOS bug. The rest
of the document is the investigation that got there.

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
   all**. Observed on PR astubbs#108, run `30785700182`, job `91598708692`.

This is the **second** test to disable the PIT lane this way — PR astubbs#101 fixed
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
broker never accepted — were just fixed on the *consumer* commit path in astubbs#100 and astubbs#108, and both
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
    producerWrapper.send(Mock for ProducerRecord, hashCode: 99235024, ...)     <- produce 1
    producerWrapper.send(Mock for ProducerRecord, hashCode: 545770597, ...)    <- produce 2
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
not yet recorded complete.** The only remaining question was §4 — whether that window exists in
production or only in this test harness — and §11 answers it: **test harness only.**

---

## 4. Hypothesis — CONFIRMED (see §11): the test releases the produce lock earlier than production does

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

`AbstractParallelEoSStreamProcessor.runUserFunction` - its `try`/`finally` around
`runUserFunctionInternal`:

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
  `WorkContainer.onPostAddToMailBox` and the error/stale paths `handleStaleWork` and the failure branch that
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

PIT's coverage pass runs instrumented and single-minion: 362 seconds to compute coverage on the astubbs#108
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

- The scoped run: `./mvnw -o -pl parallel-consumer-core -Dtest=ProducerManagerTest surefire:test`
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
| `parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/internal/AbstractParallelEoSStreamProcessor.java` | `runUserFunction` / `runUserFunctionInternal` / `cleanUpContext` — the production ordering |
| `parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/state/WorkContainer.java` | `onPostAddToMailBox`, the other `finishProducing` caller |
| `docs/inflight.md` | the ledger entry for this flake — **update it as you go**. (Pointer repair: that single file became the directory [`docs/inflight/`](../inflight/) on 2026-08-04 and was deleted in `0de96fc` — `git show 0de96fc^:docs/inflight.md`, grep `producedRecordsCantBeInTransactionWithoutItsOffsetDirect`, for the entry as it stood. It was already marked FIXED by astubbs#110 there and did not carry into `docs/inflight/`, so nothing live succeeds it. Two look-alikes that are **not** this entry: [`docs/inflight/bug-producing-lock-double-release.md`](../inflight/bug-producing-lock-double-release.md), the separate open follow-up §11 raised, and the same test name in [`docs/inflight/test-untracked-ci-flakes.md`](../inflight/test-untracked-ci-flakes.md), a later `BlockedThreadAsserter` timing defect.) |

## 10. Context worth having

- **[`docs/inflight/`](../inflight/) is the repo's ledger** for parked/in-flight work. Record findings
  there, not in a scratch file — one file per item, conventions in
  [`docs/inflight/AGENTS.md`](../inflight/AGENTS.md). (This plan said `docs/inflight.md`, the single
  file that preceded the directory.)
- **The astubbs#100 / astubbs#108 pattern.** Both were "an offset recorded as committed when it was not", on the
  consumer path, and both were mis-framed at first (astubbs#100 as CI load, astubbs#108 as an already-handled
  exception). The house lesson from that workstream: when a test fails under stress, establish
  *contention vs genuine bug* before touching the test. A weakened assertion here would hide an EOS
  violation.
- **Don't weaken assertions.** If an exception or interaction is unexpected, classify it — never
  broaden the matcher to make it pass.
- **PIT is currently a canary.** Until this is fixed, the mutation lane reports suite stability, not
  mutation coverage. Worth restating in any PR that touches it.


---

## 11. RESOLUTION (2026-08-03)

### Verdict: test-harness artifact. Production is correct.

The §4 hypothesis was right, and it was confirmed by a **controlled experiment** rather than by the
fix appearing to work — which matters, because a fix that works is not evidence of the cause.

**The experiment.** Inject an identical 400ms delay on either side of the test's `unlock()`. Same
added latency; only the position differs:

| Variant | Result |
|---|---|
| delay **after** `unlock()` — widens the window between release and `addToMailbox` | **8/8 FAIL** |
| delay **before** `unlock()` — same latency, but inside the lock, so no window | **8/8 PASS** |

Baseline was ~1 in 6. The control arm rules out "it is just slower under load", which is what every
previous look at this flake concluded.

**Note on a false negative.** The first run of this experiment showed no effect. That result was void:
`./mvnw -pl parallel-consumer-core` without `-am` fails the `ReactorModuleConvergence` enforcer rule,
so the test never recompiled and both arms ran the stale class. Anyone re-running this must confirm
`BUILD SUCCESS` on the compile step, not just look at the test outcome.

### The causal chain, end to end

1. The test hand-rolled its user function and called `beginProducing(mock(PollContextInternal.class))`
   — a **mock** context, so the production release machinery had no lock registered against the real
   one.
2. It released in its own `finally`, **inside** the user function — before
   `runUserFunctionInternal` reaches `addToMailBoxOnUserFunctionSuccess`.
3. That opens a window: produce read lock free, work not yet in the controller's inbound queue.
4. The controller (`AbstractParallelEoSStreamProcessor#controlLoop`) takes the commit write lock
   (`maybeAcquireCommitLock`), drains a mailbox that does not yet contain the completion
   (`processWorkCompleteMailBox`), then collects offsets.
5. It commits `offset=1, metadata='bgAA'` — after both `send` calls. PC's encoding was *correct*: at
   that instant offset 1 genuinely was incomplete. It was asked at the wrong moment.

**Production closes this window in two places**, and `WorkContainer#onPostAddToMailBox` states the
invariant outright:

> *"Only unlock our producing lock, when we've had the WorkContainer state safely returned to the
> controllers inbound queue, so we know it'll be included properly before the next commit as a
> succeeded offset. As in order for the controller to perform the transaction commit, it will be
> blocked from acquiring its commit lock until all produce locks have been returned, inbound queue
> processed, and thus their representative offsets placed into the commit payload (offset map)."*

`ParallelEoSStreamProcessor#pollAndProduce` puts the lock in the real context via
`context.setProducingLock(...)`; release happens at `onPostAddToMailBox` (post-mailbox by
construction) and in `cleanUpContext` in the `finally`. The test was at neither.

### The fix

`ProducerManagerTest.producedRecordsCantBeInTransactionWithoutItsOffsetDirect` now acquires against
the **real** context and hands the lock to it, exactly as production does, with no manual unlock. This
is what the test's own pre-existing TODO asked for (*"this unlocks the produce lock too early - should
be after WC returned"*). The dead `producingLockRef` went with it.

**Result: 12/12 green**, against a ~1-in-6 baseline.

### The guard

A fix that only removes today's instance invites tomorrow's. The test now asserts that the produce
lock is **still owned by the context when the user function returns** — that ownership is precisely
what defers release to `onPostAddToMailBox`. Reintroduce manual lock management and it fails
deterministically instead of returning as a 1-in-6 flake.

The guard was verified by negative control: clearing the lock from the context makes the test fail
(exit 1), so it is a real assertion and not decoration.

### Expected knock-on: the PIT lane

This was the only test failing without mutation, so `Mutation (PIT, scoped)` should now get past
coverage calculation and actually score mutants — for the first time since this flake appeared.
**Confirm that** (`bin/ci-mutation-test.sh`) rather than assuming it; a PIT lane that is still red
means something else is also unstable.

### Open question, deliberately not chased

In transactional poll-and-produce, both `onPostAddToMailBox` (via `finishProducing`) and
`cleanUpContext` release the same `ProducingLock`, and nothing clears
`PollContextInternal#producingLock` in between. On a `ReentrantReadWriteLock.ReadLock`, a second
`unlock()` by a thread holding zero read locks throws `IllegalMonitorStateException`. No such
exception appeared in any of the 12 runs, so something prevents it — but **I did not establish what**,
and an attempt to count acquire/release pairs via debug logging was itself invalid (`surefire:test`
alone does not reprocess test resources, so the logging change never reached `target/test-classes`).

Either both paths do not in fact both fire, or an exception is being swallowed somewhere a test would
not notice. Worth a look on its own, independently of this fix — it is unrelated to the flake and the
fix does not depend on the answer.

**Answered (2026-08-07): both paths fire, and the exception was being swallowed.** The second guess in
the paragraph above was the right one. `ProducingLock#unlock()` logs *after* the unlock, so a release
that threw left no line in the log and a later acquire-versus-release count read a clean 1:1 while
half the releases were failing; the `IllegalMonitorStateException` itself went into the worker's
`Future`, which nothing in main ever reads. Under `batchSize >= 2` it was a live defect, not just
noise — see `ProduceLockReleaseTest`. `cleanUpContext` is now the single release point.
