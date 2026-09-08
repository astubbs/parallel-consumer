# The public API has no thread-safety contract, so nothing can clear the 1.0 blocker

<!-- inflight-type: task -->
<!-- inflight-impact: reliability -->
<!-- inflight-labels: concurrency -->
<!-- inflight-state: deferred - after v6. astubbs#139 is not v6 scope (owner ruling, 2026-09-08). The audit below stands as the record, with its volatility premise corrected on the same day so a later reader does not inherit it; the atomicity race and the missing per-method contract are what the blocker rests on now -->


astubbs#139 (mirroring confluentinc#186) is labelled *blocker* and *1.0*, and has been since 2022,
but it names no method, no thread and no check. **That is the thing to fix first.** Below is the
audit it was missing, so the label can either be cleared or, for the first time, be actionable.

## What the mirror body gets wrong now

- Its `## Fork status` cites `file:line`, and every one of those numbers has drifted. The reasoning
  around them still holds; the coordinates do not. Anything quoting them should re-grep.
- **"Thread-safe here only by convention" is too generous.** `subscribe` is the *safest* of the
  cross-thread methods, because a pre-start subscribe genuinely is single-threaded. The unsafe
  surface is the run-state machine, and the body does not mention it.
- It frames the whole issue as needing the astubbs#142 thread-model rework. Most of the audit below
  is fixable without it.

## The audit - what a user may call off the constructing thread, and what happens

Verified against the tree, not inferred. The field at the centre of it is
`AbstractParallelEoSStreamProcessor`'s `State state`, which was **not** volatile when this audit was
written and **is** on master today (`private volatile State state = State.UNUSED`). That closes the
visibility half of every row below that named it; the atomicity half - a check-then-set with no lock
- is untouched, and is what the blocker rests on. `controlThreadFuture` is still not volatile.

<!-- post-merge: checked-begin -->
| Surface | Verdict |
|---|---|
| `pauseIfRunning` / `resumeIfPaused` | **Unsafe.** A check-then-set that is not atomic (`if (this.state == State.RUNNING) { this.state = State.PAUSED; }`) - a `pause` racing a `close` can write `PAUSED` over `CLOSING` and resurrect a consumer that was shutting down. The read is volatile now; that does not make the pair atomic |
| `close*` (six overloads on `DrainingCloseable`) | Same field. `waitForClose` spins on `while (!state.equals(CLOSED))` and the control task loops on `while (state != CLOSED)` - both read a volatile field now, so the spin terminates; the transition race above is what remains |
| `isClosedOrFailed` | Reads the same field plus a non-volatile `controlThreadFuture` |
| `subscribe` x4 | Safe before `poll*`, unsafe after - `consumer.subscribe(topics, this)` runs on the caller's thread while the poll thread owns the `KafkaConsumer`. This is the half confluentinc#346 addressed |
| `addLoopEndCallBack` | Registers onto `controlLoopHooks`, walked by `this.controlLoopHooks.forEach(Runnable::run)` every control pass. **Safe since astubbs#267** - it was a plain `ArrayList` and is now a `CopyOnWriteArrayList`, so a registration racing the walk no longer throws |
| `setLongPollTimeout` | An instance method writing `BrokerPollSystem`'s `private static Duration longPollTimeout` - one PC changes every PC in the JVM |
| `getFailureCause` | **Safe** - `failureReason` is volatile, and the comment above it says why |
| `workRemaining` | **Safe.** Predicted unsafe and refuted: `PartitionStateManager.partitionStates` is a `ConcurrentHashMap` and `PartitionState.incompleteOffsets` a `ConcurrentSkipListMap` |
| `getPausedPartitionSize` | **Safe** - `ConsumerManager.pausedPartitionSizeCache` is volatile |
| `requestCommitAsap`, `notifySomethingToDo` | Guarded already |
<!-- post-merge: checked-end -->

Two more sites are the same defect and already have owners: `PCMetrics.registeredMeters` and
`WorkManager.successfulWorkListeners`, both in
[`bug-shared-collections-across-the-poll-boundary.md`](bug-shared-collections-across-the-poll-boundary.md).

**`RetryQueue` was the sharpest example and has since been FIXED**, which is why it is kept here
rather than deleted: it is the worked case for why a javadoc claim is not a contract. Its class
javadoc said "Implementation is thread safe and uses ReadWriteLock" while `removeAll`'s fast path
read `unique.isEmpty()` with no lock held, so the JMM permitted a stale `true` and the method could
return having removed nothing - leaving a container in the retry queue while it was also in flight.
astubbs#268 found it independently and had to forbid its own snapshot source from calling the
accessors. Fixed by `d2e00faf0`; every read now takes the read lock, and the method carries a comment
explaining why the guard is on the caller's list instead.

**Checked at HEAD before writing this**, because an earlier revision of this note asserted the bug in
the present tense after the fix had already landed via a master merge on the same branch - grep
`GUARD ON THE CALLER'S OWN LIST` to see the current state. One stale copy of the old claim survives
in `parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/AGENTS.md` ("`size()` reads the map
under no lock at all"), which is master's file and out of scope here.

## Definition of done, so the label means something

1. Every method on `ParallelConsumer`, `ParallelStreamProcessor` and `DrainingCloseable` carries a
   javadoc sentence saying which threads may call it. Not a blanket claim - a per-method one, since
   `subscribe` and `pauseIfRunning` genuinely have different answers.
2. The run-state machine survives a concurrent `pause`/`resume`/`close` without losing a transition.
   Volatile is necessary and not sufficient - the check-then-set needs to be atomic.
3. No public method claims thread safety it does not have (`RetryQueue`).
4. A test that drives the surface in step 1 concurrently against a live instance and asserts no
   exception and no lost transition. Without step 4 there is nothing to say the blocker is cleared.

## Is it still a 1.0 blocker

**Yes, but for a smaller reason than the label implies.** The full confluentinc#346 ambition -
exposing *all* `Consumer` APIs safely - is a feature, and belongs with astubbs#158 rather than on a
blocker. Steps 1-4 are the blocking part: they stop the published contract from being a guess, and a
contract cannot be corrected after a 1.0 without breaking someone.

## What has already moved, and what it collides with

<!-- post-merge: checked-begin -->
- **`state` is volatile on master** and `controlThreadFuture` is not; the visibility half of the
  state defect is closed for the field that matters and the atomicity half is not - `pauseIfRunning`
  and `resumeIfPaused` are unchanged. astubbs#226, which this note once credited with the volatility
  change, is the health-check surface PR today and does not carry it.
- **astubbs#267** closed the listener-registration half.
- **astubbs#268** is the worked example of the design rule this issue needs: it marshals every read
  onto the control thread via the loop-end callback, and its `DirectStateSource` javadoc enumerates
  what may never be read from another thread, with a reason each. Whoever writes step 1 should start
  from that list rather than re-deriving it.
- **astubbs#51** (a copy of confluentinc#908), which rewrote `synchronized` blocks in the same class
  as `ReentrantLock` to avoid virtual-thread pinning, is CLOSED and no longer live work - so the
  lock-primitive question an atomicity fix has to answer is open again rather than decided by a
  colliding branch.
- confluentinc#346 itself is closed unmerged, on pre-rename packages, and predates the fork's actor
  work. Treat it as a design reference for the `subscribe` half only, not a cherry-pick.
<!-- post-merge: checked-end -->

[`docs/refactoring.md`](../refactoring.md) carries the same issue under "Thread-safe public API
surface", plus the SpotBugs `AT_*` findings that overlap it; this note is the audit and the
done-definition it points at.

## Delete when

The four steps above are done, or the surface audit moves into published javadoc and this note has
nothing left that the code does not say.
