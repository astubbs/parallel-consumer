# astubbs#116 - what the multi-agent review left open

Full report and per-reviewer JSON were in a scratch dir that is gone; this is the durable residue.
Reviewed at `fdbaaf476`. Eight reviewers, one validation pass. Verdict was **not ready** - not
because the fix is wrong (the `Spliterator` diagnosis and the live-stream design both survived
review) but because two review threads block merge and one of them attacks the regression proof.

**Do them in this order.** The first is the only one that can change what the fix has to prove.

1. **The `JStreamLiveResultStreamTest` thread (unresolved, blocks merge).** `consumerReady`
   counts down *before* `forEach` enters the spliterator, so awaiting it does not establish that
   the consumer saw an empty queue. Normal interleaving still goes RED against the old bridge, but
   it is not guaranteed - and the test's own javadoc claims it is
   (`Against the old bridge this fails with zero results collected`). Deterministic fix is smaller
   than the thread's suggested instrumented queue: gate the user function on a `CountDownLatch` so
   nothing can reach the queue until the test releases it, assert the consumer is still alive, then
   release. Also correct the javadoc. Counter-argument worth making instead: the property *is*
   proven deterministically one level down by
   `Java8StreamUtilsTest.anEmptyQueueDoesNotEndTheStreamWhileTheSourceIsRunning`, so this test is
   wiring, not the guard.
2. **The `VertxTest` `getResults` thread (unresolved, blocks merge).** Claims `closeDrainFirst()`
   stalls on the permanently-failing request that `failingHttpCall` and
   `transportFailureIsDistinctFromANonSuccessStatus` leave queued. It does not - both complete in
   under a second, because the request already failed fast and DRAIN waits for consumed work to
   *finish*. But `testVertxFunctionFail` uses `closeDontDrainFirst()` for the same shape, and a
   reviewer independently proposed matching it. Reply with the timing, mirror the non-draining
   close in the helper, resolve.
3. **`volatile` on the termination signal.** `AbstractParallelEoSStreamProcessor`'s `state` and
   `controlThreadFuture` are non-volatile and unsynchronised, and are now read from the consumer
   thread via `isClosedOrFailed()`. The closing thread never takes the queue lock the consumer polls
   on, so nothing publishes `CLOSED`. It works today only because the 100ms poll's lock stops the
   compiler hoisting the read - an accident of `LinkedBlockingQueue`'s internals, not something this
   code states. Two keywords.
4. **No test for the self-close-on-error path.** `Java8StreamUtils`' own comment justifies
   `isClosedOrFailed()` over a poison pill *because* the control thread can self-close on an
   unhandled error. Every test terminates through an explicit close, and `Java8StreamUtilsTest`
   drives the predicate with a plain boolean. The path the design argument rests on is unexercised.
5. **`VertxBaseUnitTest`'s new `@AfterEach` swallows every close failure.** Sharper than it looks: a
   subclass `@AfterEach` runs before the base one, so the unconditional catch also leaves the base
   class's guarded close a no-op on the already-CLOSED state. Genuine teardown failures are now
   hidden across every Vert.x test. Guard the swallow on already-closed-or-failed, mirroring
   `AbstractParallelEoSStreamProcessorTestBase`.
6. **`docs/features/result-models.yaml` still labels this PR the JStream deprecation.** The same
   diff rewrote the adjacent `boundaries` bullet and left
   `label: JStream deprecation and its reasoning` pointing here. Same drift in
   `docs/data/roadmap.yaml`, whose `Removing the JStream API entirely` entry still tracks astubbs#116.

**Open decision, nobody else's to make:** the returned `Stream` changed from "returns almost
immediately" to "blocks until close" on a published API, with no version signal or overload. That is
the correct fix and it should stay - the question is whether it ships with a version bump plus
release-note callout, a bounded/timeout overload for callers who cannot move consumption to their own
thread, a long-wait WARN so the hang describes itself, or nothing. Deprecating the JStream module at
all is still deliberately undecided and is a separate question.

**Two review claims were checked and rejected**, so do not re-raise them: the shared
`VertxCPResultBuilder` in `JStreamVertxParallelEoSStreamProcessor` is unchanged upstream code and its
race is identical before and after this diff (pre-existing, real, out of scope); and ending the
stream on `InterruptedException` after restoring the flag is the standard idiom, not a defect.

Also still true: the queue is unbounded, so a merely-slower consumer still grows it. The PR says so.
The `shutdownNow()` branch can make `isClosedOrFailed()` true while a worker can still enqueue, so
the javadoc's "queued results are delivered, not discarded" is stated more strongly than that path
supports.
