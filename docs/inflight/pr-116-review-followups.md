# astubbs#116 - what the multi-agent review left open

<!-- inflight-type: task -->
<!-- inflight-impact: stranded-work -->

<!-- post-merge: exempt-file -->
<!-- This note IS the PR-116 follow-up list, so every mention of astubbs#116 in it names the note's
     own subject rather than making a present-tense claim about an open branch. It is deleted when
     the last item below is closed. -->

Full report and per-reviewer JSON were in a scratch dir that is gone; this is the durable residue.
Reviewed at `fdbaaf476`. Eight reviewers, one validation pass. Verdict was **not ready** - not
because the fix is wrong (the `Spliterator` diagnosis and the live-stream design both survived
review) but because two review threads block merge and one of them attacks the regression proof.

**Do them in this order.** The first is the only one that can change what the fix has to prove.

1. ~~**The `JStreamLiveResultStreamTest` thread.**~~ **DONE, 2026-09-03, thread resolved.** The
   finding held: `consumerReady` counted down before `forEach` entered the spliterator, so awaiting
   it proved only that the thread had started. The user function is now held shut until the consumer
   is parked on an empty queue, so that state is established rather than raced for, and the wait for
   it is the assertion. Red-proven: restoring the old bridge fails it in 10s naming the alias
   `the result consumer to park on an empty queue`. The instrumented queue the thread proposed was
   not needed - `forEach` does nothing else that waits, so thread state is a sufficient signal and
   costs no production seam. The javadoc's `fails with zero results collected` claim, which the old
   shape did not have, is corrected and now records what the previous version could not establish.
   This also answers Antony's 2026-09-01 comment asking whether the test covered the timing.
2. ~~**The `VertxTest` `getResults` thread.**~~ **DONE, 2026-09-03, replied `not-addressing` and
   resolved.** The stall the thread describes does not happen, and the measurement is the opposite
   of its prediction - the two tests it names as stalling are the two FASTEST in the file:

       failingHttpCall                                 0.433s   closeDrainFirst()
       transportFailureIsDistinctFromANonSuccessStatus 0.439s   closeDrainFirst()
       testVertxFunctionFail                           2.971s   closeDontDrainFirst()

   **This entry previously said "mirror the non-draining close in the helper, resolve", and that
   half is now withdrawn** - written before anyone measured. Two reasons not to make the change.
   `testVertxFunctionFail` does not use `closeDontDrainFirst()` to dodge a stall, which is what the
   thread assumes; its own comment says it reads `workRemaining()` first because *closing drains the
   retries away*, a different requirement this helper does not have. And
   `transportFailureIsDistinctFromANonSuccessStatus` asserts `assertCommits(of())` AFTER
   `getResults`, so draining is what gives that assertion a quiescent point - going non-draining
   would let "nothing committed" pass merely because the commit had not happened yet, turning a real
   assertion into a race.
3. **`volatile` on the termination signal - HALF-CLOSED by master, 2026-08-31 merge.**
   `AbstractParallelEoSStreamProcessor`'s `state` and `controlThreadFuture` were both non-volatile and
   unsynchronised, and are now read from the consumer thread via `isClosedOrFailed()`. The closing
   thread never takes the queue lock the consumer polls on, so nothing published `CLOSED`. It worked
   only because the 100ms poll's lock stopped the compiler hoisting the read - an accident of
   `LinkedBlockingQueue`'s internals, not something the code stated. Master's astubbs#342
   (`8455a9c3e`) made `state` volatile for its own reasons, which publishes the `state == CLOSED`
   half. `controlThreadFuture` is still a plain field and `isClosedOrFailed()` does read it, so that
   half is unchanged - but its failure mode is now benign rather than a hang: a consumer thread seeing
   a stale `Optional.empty()` falls through to the volatile `state` read, which is correct. One
   keyword left, no longer load-bearing for this fix.
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
