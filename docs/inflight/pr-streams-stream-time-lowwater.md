# Stream time on the PC path: what landed, and what U13 still owes

Branch `feats/ks-streams-stream-time-lowwater`, on top of
`feats/ks-streams-task-lifecycle-and-rebalance`. Plan:
`docs/plans/2026-08-11-002-feat-ks-streams-stream-time-lowwater-plan.md` (astubbs#255, master-plan U13).

## What landed

Stream time advances on the PC path. `PcTaskDispatcher` publishes a **low-water mark** - the lowest
stream timestamp still in flight, or the highest ever dispatched when nothing is, clamped monotone -
and the patched `StreamTask` reads it from `maybePunctuateStreamTime`, `canPunctuateStreamTime` and
`streamTime()`. That last one is what `ProcessorContext.currentStreamTimeMs()` delegates to, so a
**public API that returned a constant -1** on this path now returns a real value.

The extracted timestamp crosses the seam in `WorkPreparer`'s return value (`PreparedRecord`), not off
the `ConsumerRecord` - those are different numbers whenever a topology configures a `TimestampExtractor`.

Verified: Kafka's own 419 with the seam off, zero failures; module unit suite green, including nine new
stream-time tests in `PcTaskDispatcherTest`.

## What U13 still owes, in priority order

1. **U13.3's integration test.** There is no end-to-end proof: no real topology, no real broker, no
   `STREAM_TIME` punctuator observed firing, and no seam-OFF control arm. This matters more than it
   looks, because of item 2.
2. **U13.4's divergence measurements.** How far punctuation lags stock, whether the lag is bounded by
   the slowest in-flight record, whether two runs over identical input punctuate at the same points,
   and the **lateness meter split by cause** (dispatch-order term versus not-yet-fetched term).
   `getDispatchesBehindStreamTime()` exists and is unread outside tests - it is the instrument waiting
   for its experiment. The split is what U14's task-idling gate decision needs.
3. **U13.5's reinstatement ledger.** Eight of thirteen `PcUnsupportedConstruct` reasons still argue
   that stream time "never advances on the PC path", which is now false, and there are **42 occurrences
   in `pc-streams.patch`** of the same claim in the `@DoNotCall`/`@deprecated` strings a user sees as a
   compile error. Note the detector the plan specifies must match the *claim*, not one literal string:
   a grep for "never advances on the PC path" returns **zero** hits in the patch.
4. **U13.6's divergence records**, including the README's known-gaps text and `CONCEPTS.md`.

## Coverage holes the testing review named, and none of these are U13.4's integration test

The one it found first is **fixed**: no test could tell the extractor's timestamp from the broker's,
because every stream-time test derived one from the other through `PreparedRecords.prepared`. Swapping
the production read to `work.getCr().timestamp()` left all nine green. Now pinned by
`theMarkFollowsTheTimestampThePreparerSuppliedNotTheBrokersOwn`, and the pin is **mutation-verified**:
with that swap applied, exactly one test goes red and it is that one.

Correctness review found a second one, also **fixed**: the STREAM_TIME punctuator warning named two
divergences and stopped immediately before the one that can corrupt state. `punctuate()` runs on the
StreamThread while up to `poolSize` records are still inside the chain on workers, so **stock's guarantee
that `punctuate()` and `process()` never overlap for one task is gone** - and a plain `KeyValueStore` is
*supported* here, so the obvious punctuator ("iterate the store now that everything up to T is done")
races the processors writing it. The warning now carries that as clause 3.

**The open question that clause raises, and this plan does not settle:** whether a STREAM_TIME punctuator
should be *refused* when the topology also declares a state store, rather than warned about. A warning is
the right call while the punctuator is the only thing that can reach the hazard; it stops being the right
call if anyone finds a way to hit it without registering one.

Adversarial review found the worst one, also **fixed**: **"never ahead of stock" was false**, and the
counter-example was in this branch's own history. Writing the pool-of-one test measured `[0, 2, 2]`
against stock's `[0, 1, 2]` on one partition with three keys; the response at the time was to narrow the
test to a single key so it passed, deleting the only coverage of the divergence. Both javadocs now state
the narrower property that is actually true - the mark never passes a record *currently in flight* - and
`theMarkOvertakesStockWhereDispatchOrderDiffers` pins the `[0, 2, 2]` sequence.

**Punctuation and commit coverage - what is left of it after measurement.** This entry used to rank the
largest open item as *a punctuator's effects never become commit-covered*, with
`hasUncommittedWork() || commitNeeded` as the one-line candidate. Measured on
`feats/ks-streams-punctuator-commit-coverage` and `feats/ks-streams-postcommit-checkpoint-gap`, that
framing does not hold: offsets commit on the PC path, and `postCommit` runs under load (12,000 records
checkpointed at changelog 11,862-11,929 against stock's 11,999). What survives is an idle-window tail -
when no work completes between the commit and `TaskExecutor`'s second loop, that round's `postCommit` is
skipped, bounded by the final commit round, with clean `close()` checkpointing regardless.

Still open from it:

- ~~Whether a punctuator's own effects survive a crash on the PC path.~~ **Measured: they do.**
  `feats/ks-streams-punctuator-effect-survival` punctuates three times with the commit interval at
  Kafka's 30s default and aborts ~600ms in, so no commit is possible in the window - all three forwards
  reach the output topic and all three store writes reach the changelog. The producer carries them
  without `flush()`. **Non-EOS only:** under exactly-once the forward sits in an open transaction, which
  is the one configuration where this bites; EOS is refused (U11) and out of scope (KTD7).
- **The re-fire-over-covered-event-time claim**, still unmeasured.
- **WALL_CLOCK_TIME punctuators fire here unwarned**, where STREAM_TIME logs. Pre-existing, not
  introduced by U13.
- **The `hasUncommittedWork() || commitNeeded` candidate cannot be evidenced through commit cadence.**
  The thread-level `commit-total` sensor is pinned at zero on an idle PC task, so it carries no
  information about punctuation. `PunctuatorCommitCoverageTest`'s javadoc records why, plus two further
  observables (the checkpoint file, punctuator output on a topic) that look decisive and are not - read
  it before picking an observable.

Still open:

- **`seedStreamTime`'s call site has no coverage at all.** Delete the
  `if (pcDispatcher != null) { pcDispatcher.seedStreamTime(...) }` block from the patch and the build
  stays green. The unit test drives `seedStreamTime` directly, so it pins the arithmetic and not the
  wiring; and the only execution that runs Kafka's `shouldReadCommittedStreamTime*` cases pins the seam
  **off**, so the branch never executes there. Consequence: a task restarted against a stock-written
  group restarts stream time at -1 and re-fires punctuators over closed windows, with nothing red. The
  suggested shape is a third, narrow surefire execution running only the stream-time cases with the seam
  on - which is also what would turn `pcAwareStreamTime()`'s PC branch from measured-by-hand into gated.
- **`pcAwareStreamTime()`'s PC branch is unexercised by any automated run**, for the same reason. That
  covers `ProcessorContext.currentStreamTimeMs()` as well as punctuation. Distinct from the missing
  end-to-end topology test: the seam-on arm of Kafka's own `StreamTaskTest` already exists as a
  measurement and has simply never been made a gate.
- **The warn-once `STREAM_TIME` punctuator branch** in the patched `schedule()` has no test - and it is
  the only channel telling a user their punctuator's output sits outside PC's commit frontier.
- **`aRecordDroppedDuringPreparationContributesNoTimestamp` cannot observe its own claim.** With every
  record dropped, `dispatchedToPool` is 0, so the publish is gated off and the mark could not have moved
  whatever the implementation recorded into `maxDispatchedStreamTimestamp`.
- **`releaseAndDrain` synchronises on `getInFlightCount()`**, which works only because `runOnWorker`
  enqueues the completion before decrementing. `runOnWorker`'s own comment schedules that order for
  change; when it changes, `theMarkFollowsTheLowestTimestampStillInFlight` starts flaking and the blame
  will land on stream time.
- **The clean-close-with-forced-pool branch** - `dropStreamTimeHoldsWithoutPublishing()`'s actual claim -
  is untested; only the abort path is. And the abort test goes red if `abortClose()` gains a publish, but
  stays green if it gains the map clear whose absence is the point.
- **"Exactly what stock holds" is asserted about the dispatcher and only described as equal to stock.**
  No stock value is computed anywhere in these tests, so the never-ahead-of-stock property still rests on
  prose. U13.4's seam-OFF control arm is what changes that.
- **The new extracted-timestamp test pins the dispatcher, not `pcPrepare`.** It proves the plumbing
  carries whatever it is given; nothing proves `StreamTask.pcPrepare` gives it `stamped.timestamp` rather
  than `rawRecord.timestamp()`. Same gap as the two above: it needs a test on the StreamTask side.
- **No test pins the `dispatchedToPool > 0` guard** - a pump that dispatches nothing because the pool is
  full must leave the mark unchanged. That is the pump where the map is fullest.
- **`seedStreamTime` arriving while records are in flight** is untested; only the empty-pool case is.
- **The seed reaches the dispatcher but not `pcRecordQueues`.** `RecordQueue.updateHead` passes its own
  `partitionTime` to `timestampExtractor.extract(record, partitionTime)`, and the PC path's queues are
  created lazily at UNKNOWN. A topology using Kafka's shipped `UsePartitionTimeOnInvalidTimestamp` throws
  on the first record after a restart with an invalid embedded timestamp, where stock recovers from the
  committed value. Candidate fix is one line beside the `seedStreamTime` call:
  `pcRecordQueues.computeIfAbsent(partition, recordQueueCreator::createQueue).setPartitionTime(committedTimestamp)`.
- **`close()`'s drain can advance the mark over work a forced shutdown killed**, which is precisely what
  `dropStreamTimeHoldsWithoutPublishing()` is named for preventing. `awaitTermination` expires,
  `shutdownNow()` interrupts the chains, each throws into `recordFailure`, and the very next statement is
  `drainCompletions()` - which releases those holds and publishes. Bounded today only because a closed
  dispatcher has no reader. The clean fix is a freeze flag checked in `publishStreamTime()`, set at the
  top of both close paths.
- **`RejectedExecutionException` from `workerPool.execute` leaks a hold.** `holdStreamTime` and the
  `inFlight` increment both happen before the submit, with no compensation - so an `abortClose()` racing a
  pump (which `abortAllActive()` does by design) pins the mark at that record's timestamp forever. The
  `inFlight` half predates U13; the stream-time half is new and converts a wrong counter into a stopped
  punctuation clock.
- **`PreparedRecord` validates neither field.** A preparer returning a null `Runnable` gets an NPE
  misclassified as a *processing* failure, which with retries disabled blocks that KEY shard for the life
  of the task and reports a topology error. This is the module's public extension seam;
  `Objects.requireNonNull` at the constructor is the right place to fail.
- **The `__processing.threads.enabled__` hazard is now a hash-table race, not a stale long** - and
  `PcSupportedEnvelope` already refuses a *configuration* (EOS), so converting this from an accepted
  silent hang into a named refusal is one boolean away.
- **STREAM_TIME punctuations may be SKIPPED, not merely delayed.** `PunctuationSchedule.next` collapses
  every interval crossed in one jump into a single firing; on this path the jump is the normal case
  rather than a data gap. The warning says "firing times lag", which does not convey that.

Two things correctness review checked and cleared, worth not re-deriving: both publish guards **are**
equivalent to unconditional publishing (a skipped publish would need a hold added-then-drained, and the
drain publishes), and `abortClose()` not clearing the map holds up - after abort no publish is reachable,
because `dispatchAvailable` drains then early-returns on `closed`, and a late outcome's `remove()` returns
null so `releasedAnyHold` stays false. Also: `TopicPartitionMetadata.decode` cannot hand `seedStreamTime`
a bogus huge value from a PC-written commit, because `OffsetEncoding`'s magic bytes deliberately exclude 1
and 2 and the decode falls through to `UNKNOWN`.

## The thing to read before trusting a number here

**Kafka's upstream suites cannot measure this unit, and that is a finding rather than an excuse.**

- `StreamTaskTest` seam-ON is **30 failing before and after**, identical case-for-case (N=3 each side).
  Both pile F cases stay red for reasons that are not stream time:
  `shouldPunctuateOnceStreamTimeAfterGap` fails at `:1209` on `numBuffered()` (pile C, U14's), and
  `shouldRespectPunctuateCancellationStreamTime`'s failure **moved** from the stream-time assertion at
  `:1303` to the asynchronous-dispatch one at `:1307` - and it was observed green in one run. Racy;
  recorded UNRESOLVED.
- `StreamThreadTest` seam-ON read **41, 37, 37 and 1** across four runs of byte-identical code. It is
  not a measurement. An interim draft of the plan reported "41 to 37" as a win; retracted.

So the evidence that stream time advances is currently indirect plus the module's own unit tests. Item
1 above is what closes that.

## Traps this work hit, worth not re-learning

- **`-Dincluded.groups=<nonexistent>` empties the module's DEFAULT surefire execution.** It is the right
  way to isolate the *upstream* execution, and it silently skips the module's own tests at the same
  time. A run that looked green with "419, zero failures" had not run a single new test.
- **`-Dtest=...` leaves stale XML in `surefire-reports-kafka-upstream/`.** It overrides the upstream
  execution's `<includes>`, so the module's own classes get written into that directory and are still
  there on the next reading. Check file timestamps before trusting a parse of that directory.
- **The seam-ON gate does work from the CLI**, despite the pom pinning
  `pc.streams.dispatch.enabled=false` in that execution's `<systemPropertyVariables>` - Surefire copies
  command-line user properties last. Prove it per run: `PC dispatch active for task` appears 122 times
  in a seam-ON run and zero in a seam-OFF one.
- **Publishing the mark per record is wrong**, and it looks right. The monotone clamp would fix the mark
  on a partial batch: hold 100, publish 100, then hold 50, and the clamp refuses to come back down. The
  publish has to be at the tail of the dispatch batch - which means a worker can read a mark that does
  not yet include its own record. That reads low, which is the safe direction.

## Delete when

Items 1-4 are done, or re-homed onto their own tracking with the evidence they need.
