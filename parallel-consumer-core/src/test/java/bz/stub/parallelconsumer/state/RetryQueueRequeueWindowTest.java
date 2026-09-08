package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Duration;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.PARTITION;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The <b>last</b> re-queue window: a rebalance completing between {@link WorkManager#onFailureResult}'s LIVE
 * epoch re-validation and the {@link ShardManager#onFailure} it guards.
 * <p>
 * <b>How this differs from {@link WorkManagerStaleCheckDoubleLookupTest}, which is green.</b> That test fires
 * its rebalance at staleness checkpoint 3's seam - the two-argument
 * {@code checkIfWorkIsStale(PartitionState, WorkContainer)} inside {@code handleFutureResult}. astubbs#346
 * closed that one by adding a second, LIVE check inside {@code onFailureResult}, which catches a rebalance
 * that landed at checkpoint 3 and drops the re-queue. So checkpoint 3's seam is covered. The live check is
 * itself a check-then-act - its own comment says no epoch check here can ever be atomic with the actions - and
 * <b>the gap between it and {@code sm.onFailure} is what these tests drive</b>. Same rebalance, same thread,
 * one seam later.
 * <p>
 * <b>The seam is a stand-in for the other thread, not a fixture the product knows about.</b>
 * {@code RequeueWindowWorkManager} overrides the live check, computes the real answer, then runs the full
 * production revoke path before returning it. If that call ever stops happening where these tests assume, they
 * stop exercising the window - which is why every arm asserts that the race fired AND that the live check
 * actually returned "not stale", the two preconditions that make the reproduction mean anything.
 * <p>
 * <b>What the orphan actually costs, measured rather than asserted from the note.</b> The consequence recorded
 * on {@code onFailureResult} was that {@link ShardManager#getWorkableRecords()} subtracts the parked-for-retry
 * figure from a shard population that no longer contains the orphan, misleading the broker-poller load gate
 * into a confluentinc#857-family stall. {@link #aQueueOnlyOrphanCostsTheDrainFigureAndNotTheLoadGate()}
 * measures that claim and it does not hold - see that test. The durable harm is
 * {@link ShardManager#getNumberOfWorkQueuedInShardsAwaitingSelection()}, which floors its shard term at zero
 * and therefore keeps the orphan's ready-to-retry contribution once the pipeline is drained - the figure
 * behind {@code AbstractParallelEoSStreamProcessor#isRecordsAwaitingProcessing()} and so behind
 * {@code drain()}.
 *
 * @author Antony Stubbs
 * @see WorkManagerStaleCheckDoubleLookupTest
 * @see ShardManager#onFailure(WorkContainer)
 */
@Slf4j
class RetryQueueRequeueWindowTest {

    static final String TOPIC = "myTopic";

    final TopicPartition tp = new TopicPartition(TOPIC, 0);

    /**
     * A {@link WorkManager} that completes a full rebalance in the gap between {@code onFailureResult}'s live
     * epoch re-validation and {@code sm.onFailure}.
     * <p>
     * The answer the live check gave is recorded as well as the firing, because the two preconditions are
     * different: a race that fired proves the seam was reached, and a check that answered "not stale" proves
     * the re-queue branch was the one taken. A test that only asserted the first would still pass if the check
     * had started answering "stale" for an unrelated reason, and would then be asserting nothing.
     */
    static class RequeueWindowWorkManager extends RacingSeamWorkManager {

        private int liveCheckCalls;

        private boolean lastLiveCheckSaidStale;

        RequeueWindowWorkManager(PCModuleTestEnv module) {
            super(module);
        }

        int getLiveCheckCalls() {
            return liveCheckCalls;
        }

        boolean lastLiveCheckSaidStale() {
            return lastLiveCheckSaidStale;
        }

        @Override
        public boolean checkIfWorkIsStale(WorkContainer<String, String> workContainer) {
            boolean answerFromTheLiveMap = super.checkIfWorkIsStale(workContainer);
            this.liveCheckCalls++;
            this.lastLiveCheckSaidStale = answerFromTheLiveMap;
            fireOnceIfArmed();
            return answerFromTheLiveMap;
        }
    }

    final PCModuleTestEnv module = new PCModuleTestEnv(ParallelConsumerOptions.<String, String>builder()
            .ordering(PARTITION)
            .consumer(new MockConsumer<>(OffsetResetStrategy.EARLIEST))
            .build());

    final RequeueWindowWorkManager wm = new RequeueWindowWorkManager(module);

    {
        // install the racing double before anything asks the module for a work manager
        module.setWorkManager(wm);
    }

    private WorkContainer<String, String> aFailedRecordTakenAsWork() {
        WorkContainer<String, String> wc = ModelUtils.registerOneRecordAndTakeIt(wm, tp);
        wc.onUserFunctionFailure(new RuntimeException("simulated user function failure"));
        return wc;
    }

    private void assertTheWindowWasActuallyDriven() {
        assertWithMessage("PRECONDITION: the armed rebalance must have fired inside the live-check seam - "
                + "without it this test exercises no window at all")
                .that(wm.raceHasFired())
                .isTrue();
        assertWithMessage("PRECONDITION: the live epoch check must actually have run")
                .that(wm.getLiveCheckCalls())
                .isGreaterThan(0);
        assertWithMessage("PRECONDITION: the live epoch check must have answered NOT stale, so that the "
                + "re-queue branch is the one taken - a 'stale' answer here means the test is asserting "
                + "nothing about the window")
                .that(wm.lastLiveCheckSaidStale())
                .isFalse();
    }

    /**
     * P1 - reachability. The revoke sweep empties the shard while the controller is between its live check and
     * its add, so {@code sm.onFailure} adds to the retry queue a container that is resident in no shard.
     * <p>
     * Under PARTITION ordering the shard object survives being emptied (only KEY ordering garbage-collects an
     * empty shard), so {@code getShard} still answers present and the add goes through.
     */
    @Test
    void aRebalanceInsideTheLiveCheckWindowMustNotOrphanTheRetryQueueEntry() {
        WorkContainer<String, String> wc = aFailedRecordTakenAsWork();

        wm.arm(() -> wm.onPartitionsRevoked(UniLists.of(tp)));
        wm.handleFutureResult(wc);

        assertTheWindowWasActuallyDriven();

        assertWithMessage("PRECONDITION: the sweep must have emptied the shard, or there is no orphan to make")
                .that(wm.getSm().getNumberOfRecordsInShards())
                .isEqualTo(0L);

        assertWithMessage("a container that is resident in no shard must not be in the retry queue: work is "
                + "handed out by scanning shards, so it can never be selected, completed, or swept")
                .that(wm.getSm().getRetryQueue().contains(wc))
                .isFalse();
    }

    /**
     * P1, second half - the orphan is not merely present but, at the time this was written, unremovable: every
     * route that took an entry out of the retry queue reached it through shard contents, and the shard has
     * none.
     * <p>
     * <b>That premise no longer holds, and the arm is kept because of it rather than in spite of it.</b>
     * {@link ShardManager#purgeDepartedRetryEntries()} reaches an entry by asking the SHARD about the
     * container rather than the other way round, so the two work requests below would now collect the orphan
     * even if the confirmation on {@link ShardManager#onFailure} stopped preventing it. What this arm still
     * pins is that the window leaves nothing behind at all - and it goes red if the confirmation AND the purge
     * are both removed, which is the honest statement of what it now guards.
     */
    @Test
    void nothingCanEverRemoveTheOrphanedRetryQueueEntry() {
        WorkContainer<String, String> wc = aFailedRecordTakenAsWork();

        wm.arm(() -> wm.onPartitionsRevoked(UniLists.of(tp)));
        wm.handleFutureResult(wc);

        assertTheWindowWasActuallyDriven();

        // every sweep the engine has, run against the orphan
        long swept = wm.getSm().removeStaleContainers();
        wm.getSm().getWorkIfAvailable(100);
        module.getMutableClock().add(Duration.ofHours(1));
        wm.getSm().getWorkIfAvailable(100);

        assertWithMessage("PRECONDITION: the stale sweep must find nothing to sweep - its only route to a "
                + "retry-queue entry is the shard, and the revoke already emptied it")
                .that(swept)
                .isEqualTo(0L);

        assertWithMessage("after the stale sweep, a full shard scan, the retry delay elapsing and a second "
                + "scan, the entry must be gone - if it is still here it is here for the life of the instance")
                .that(wm.getSm().getRetryQueue().contains(wc))
                .isFalse();
    }

    /**
     * P4 - the durable harm, and the one the load gate is not.
     * {@link ShardManager#getNumberOfWorkQueuedInShardsAwaitingSelection()} is
     * {@code readyToRetry + max(0, shardCounters - retryQueueSize)}. The orphan adds one to
     * {@code readyToRetry} and one to {@code retryQueueSize}; those cancel only while
     * {@code shardCounters - retryQueueSize} is positive. Drained - which is exactly when it matters - the
     * second term floors at zero and the {@code readyToRetry} contribution survives alone, permanently.
     * <p>
     * That figure is {@code WorkManager#isRecordsAwaitingProcessing()}, which
     * {@code AbstractParallelEoSStreamProcessor#drain()} requires to be false before it transitions to
     * closing. A single orphan therefore holds a draining close open until the drain timeout expires, on an
     * instance with nothing assigned and nothing in flight.
     */
    @Test
    void anOrphanMustNotHoldTheDrainOpenForever() {
        WorkContainer<String, String> wc = aFailedRecordTakenAsWork();

        wm.arm(() -> wm.onPartitionsRevoked(UniLists.of(tp)));
        wm.handleFutureResult(wc);

        assertTheWindowWasActuallyDriven();

        // the orphan only reads as ready-to-retry once its delay elapses; from then on it never un-elapses
        module.getMutableClock().add(Duration.ofHours(1));

        assertWithMessage("PRECONDITION: nothing is assigned, nothing is in flight and the shards hold "
                + "nothing, so there is genuinely no work waiting")
                .that(wm.getSm().getNumberOfRecordsInShards())
                .isEqualTo(0L);

        assertWithMessage("drain() transitions to closing only when this is false - a permanent true holds a "
                + "draining close open until the drain timeout, with no work anywhere in the system")
                .that(wm.isRecordsAwaitingProcessing())
                .isFalse();
    }

    /**
     * P5 - what a queue-only orphan actually costs, measured on one built DIRECTLY rather than through the
     * window above, so the measurement stands whether or not the window is open. Green before and after the
     * fix: this is the characterisation, not the regression test.
     * <p>
     * <b>It refutes the consequence this defect was recorded with.</b> The comment at
     * {@code WorkManager#onFailureResult} and the in-flight note both said the orphan misleads the
     * broker-poller load gate, which reads {@link ShardManager#getWorkableRecords()}
     * ({@code inShards - parkedForRetry}), by subtracting a parked figure from a population that no longer
     * contains the orphan - a confluentinc#857-family stall. That is wrong twice over, and both halves are
     * asserted below:
     * <ul>
     * <li><b>Not permanent.</b> {@code parkedForRetry} is {@code queueSize - readyToRetry}. The orphan
     *     contributes to it only while its retry delay is still running; once the delay passes it counts in
     *     both terms and the contribution is exactly zero, for good.</li>
     * <li><b>Not the stall direction.</b> While the contribution is non-zero it makes {@code workable} read
     *     LOW, so {@code isSufficientlyLoaded()} reads false and the consumer fetches MORE. A stall needs the
     *     figure to read HIGH.</li>
     * </ul>
     * <b>The real cost is the other figure.</b>
     * {@link ShardManager#getNumberOfWorkQueuedInShardsAwaitingSelection()} is
     * {@code readyToRetry + max(0, shardCounters - queueSize)}. The orphan's two contributions cancel only
     * while {@code shardCounters - queueSize} is positive; drained, that term floors at zero and the
     * {@code readyToRetry} contribution survives alone and permanently. That is
     * {@code WorkManager#isRecordsAwaitingProcessing()}, which {@code drain()} requires to be false before it
     * transitions to closing.
     */
    @Test
    void aQueueOnlyOrphanCostsTheDrainFigureAndNotTheLoadGate() {
        WorkContainer<String, String> wc = aFailedRecordTakenAsWork();

        // build the orphan directly: the revoke takes the container out of BOTH structures, then it is put
        // back into the queue alone - exactly the state the window used to leave behind
        wm.handleFutureResult(wc);
        wm.onPartitionsRevoked(UniLists.of(tp));
        wm.getSm().getRetryQueue().add(wc);

        assertWithMessage("FIXTURE: the orphan must be in the retry queue")
                .that(wm.getSm().getRetryQueue().contains(wc))
                .isTrue();
        assertWithMessage("FIXTURE: and in no shard, or it is not an orphan")
                .that(wm.getSm().getNumberOfRecordsInShards())
                .isEqualTo(0L);

        var beforeTheDelayPasses = wm.getSm().getWorkableRecords();
        assertWithMessage("while the orphan's retry delay is still running it counts as parked, so the load "
                + "gate's figure reads LOW by exactly that - which fetches SOONER, the opposite of a stall")
                .that(beforeTheDelayPasses.getWorkable())
                .isEqualTo(beforeTheDelayPasses.getInShards() - 1);
        assertWithMessage("and the drain figure is not yet affected - the orphan is not ready to retry yet")
                .that(wm.isRecordsAwaitingProcessing())
                .isFalse();

        module.getMutableClock().add(Duration.ofHours(1));

        var afterTheDelayPasses = wm.getSm().getWorkableRecords();
        assertWithMessage("once the delay passes the orphan counts in BOTH the queue size and the "
                + "ready-to-retry count, so parked-for-retry nets to zero and the load gate is told exactly "
                + "what the shards hold - the load gate is NOT where this defect is paid for")
                .that(afterTheDelayPasses.getWorkable())
                .isEqualTo(afterTheDelayPasses.getInShards());
        assertWithMessage("the drain figure is where it IS paid for: drained, the shard term floors at zero "
                + "and the orphan's ready-to-retry contribution survives alone, so a draining close can never "
                + "transition to closing and hangs to its timeout")
                .that(wm.isRecordsAwaitingProcessing())
                .isTrue();
    }

    /**
     * The two halves of the revoke sweep, run around the controller's re-queue, so the interleave that matters
     * is placed by construction rather than raced for.
     * <p>
     * The production sweep does both in one call ({@code removeWorkFromShardFor}); these model it split, with
     * {@code sm.onFailure} landing in the middle - which is the only interleaving either arm is about.
     *
     * @param queueFirst the order the sweep does its two removals in: {@code true} models the superseded
     *                   astubbs/parallel-consumer#431 ordering, {@code false} the shard-first one production
     *                   had before its callbacks stopped touching the queue at all
     */
    private void sweepAroundTheRequeue(WorkContainer<String, String> wc, boolean queueFirst) {
        var shard = wm.getSm().getShard(wm.getSm().computeShardKey(wc)).get();
        Runnable queueRemoval = () -> {
            // Named rather than discarded, and deliberately NOT asserted: whether an entry was present depends
            // on which position this runnable is in. Queue-first finds nothing, because the controller has not
            // added yet - that IS the superseded astubbs/parallel-consumer#431 ordering and the reason its arm
            // makes an orphan. Shard-first finds the controller's add. Both are modelled behaviour, neither is
            // production's (which removes from no queue at all), so the value is recorded to show it was
            // considered rather than dropped.
            var ignoredEntryWasPresent = wm.getSm().getRetryQueue().remove(wc);
        };
        Runnable shardRemoval = () -> {
            var removedFromTheShard = shard.removeWorkAtOffset(wc.offset());
            assertWithMessage("FIXTURE: the modelled sweep must displace the container under test - a null or a "
                    + "different instance here means this arm arranges no departure at all, and everything it "
                    + "asserts afterwards is vacuous")
                    .that(removedFromTheShard)
                    .isSameInstanceAs(wc);
        };

        (queueFirst ? queueRemoval : shardRemoval).run();
        wm.getSm().onFailure(wc);
        (queueFirst ? shardRemoval : queueRemoval).run();
    }

    /**
     * <b>The confirmation on {@link ShardManager#onFailure} is ordering-dependent, and this is the arm that
     * says so.</b> It holds against a sweep whose SHARD removal is the observable one: the departure becomes
     * visible to the residency read before anything else happens, so either the controller sees a departed
     * container and undoes its add, or its add landed early enough for the sweep to have found it.
     * <p>
     * <b>Against a sweep that removed from the QUEUE first, a one-shot confirmation is defeated</b> - the
     * sweep's queue removal passes over an empty queue, the controller then adds and reads residency while the
     * container is still in its shard, and the shard removal happens afterwards. Neither party removes the
     * entry.
     * <p>
     * <b>Historical, and deliberately kept.</b> That queue-first ordering was the superseded
     * astubbs/parallel-consumer#431 design, which had the poll thread DECLINE the write lock rather than not
     * ask for it - and which therefore had to repeat the queue removal after the shard removal to close this
     * half. Production has never had that ordering and now never will: the callbacks touch the shards only,
     * and {@link ShardManager#purgeDepartedRetryEntries()} collects what they leave. The arm is kept because
     * it is what the two write-ups that cite it point at for WHY a one-shot residency confirmation is not a
     * complete answer on its own - which is still true, and is why the purge rather than the confirmation is
     * what closes the window. Its assertion stands as written: it hand-builds both removals, so nothing in
     * production moves it.
     */
    @Test
    void aQueueFirstSweepDefeatsTheOneShotConfirmation() {
        WorkContainer<String, String> wc = aFailedRecordTakenAsWork();

        sweepAroundTheRequeue(wc, true);

        assertWithMessage("PRECONDITION: the container must have left its shard, or there is no orphan to make")
                .that(wm.getSm().getNumberOfRecordsInShards())
                .isEqualTo(0L);

        assertWithMessage("MODELS THE SUPERSEDED DESIGN: with the sweep removing from the QUEUE first, the "
                + "residency read still sees a resident container and the add is not undone - so the entry is "
                + "orphaned. Production never removes from the queue in a callback, so nothing here can change "
                + "under this arm; it stands as the reason a one-shot confirmation is not the whole answer, "
                + "which ShardManager.purgeDepartedRetryEntries() is")
                .that(wm.getSm().getRetryQueue().contains(wc))
                .isTrue();
    }

    /**
     * The matched control for the arm above: the identical steps, with only the sweep's internal order
     * changed. Master's shard-first ordering keeps the pair whole, because the residency read that follows the
     * add now observes a container that has already gone.
     * <p>
     * Same magnitude, different position - this pair is what establishes that the ordering is the responsible
     * term, rather than anything else about the interleave.
     */
    @Test
    void aShardFirstSweepIsCaughtByTheConfirmation() {
        WorkContainer<String, String> wc = aFailedRecordTakenAsWork();

        sweepAroundTheRequeue(wc, false);

        assertWithMessage("PRECONDITION: the container must have left its shard")
                .that(wm.getSm().getNumberOfRecordsInShards())
                .isEqualTo(0L);

        assertWithMessage("with the sweep removing from the SHARD first, the residency read after the add sees "
                + "a departed container and takes the entry back out")
                .that(wm.getSm().getRetryQueue().contains(wc))
                .isFalse();
    }

    /**
     * <b>The mechanical coupling between the two modelled arms above and the production code they model.</b>
     * <p>
     * Both arms hand-build the sweep's two removals, so neither can fail when production's ordering changes,
     * and on its own that leaves the promised regression signal resting on a future author remembering to
     * rewrite a test. Raised in review, and correct. This arm supplies what they cannot: it drives the REAL
     * revoke sweep ({@code WorkManager#onPartitionsRevoked} -> {@code ShardManager.removeWorkFromShardFor})
     * and pins what that sweep does to the retry queue.
     * <p>
     * <b>Re-stated: the answer is now "nothing at all".</b> It used to be "the queue removal is gated on the
     * shard removal having found the container", which was true of a shard-first sweep and was what the
     * confirmation on {@link ShardManager#onFailure} depended on. The sweep runs on the broker-poll thread
     * inside {@code poll()}, so it no longer asks the retry queue for its unbounded fair write lock at all -
     * and {@link ShardManager#purgeDepartedRetryEntries()} collects what that leaves, on the controller
     * thread, discriminating by shard residency rather than by which removal ran first.
     * <p>
     * <b>The resident container is what makes that a claim rather than a tautology.</b> "Both entries survived
     * the sweep" would also pass on a sweep that never ran; the controller pass at the end separates them,
     * collecting exactly the departed one and leaving the resident one alone. So this arm goes red both if a
     * callback starts touching the queue again and if the purge stops discriminating.
     */
    @Test
    void theProductionSweepLeavesTheRetryQueueToTheController() {
        var residentPartition = tp;
        var departedPartition = new TopicPartition(TOPIC, 1);

        // PARTITION ordering shards by partition, so these two are independent and both can be in flight
        var stillResident = ModelUtils.registerOneRecordAndTakeIt(wm, residentPartition);
        var willLeaveItsShard = ModelUtils.registerOneRecordAndTakeIt(wm, departedPartition);

        for (var wc : UniLists.of(stillResident, willLeaveItsShard)) {
            wc.onUserFunctionFailure(new RuntimeException("simulated user function failure"));
            wm.getSm().onFailure(wc);
        }

        assertWithMessage("FIXTURE: both containers must be parked for retry before the sweep runs")
                .that(wm.getSm().getRetryQueue().contains(stillResident)
                        && wm.getSm().getRetryQueue().contains(willLeaveItsShard))
                .isTrue();

        var departedShard = wm.getSm().getShard(wm.getSm().computeShardKey(willLeaveItsShard)).get();
        var removedByHand = departedShard.removeWorkAtOffset(willLeaveItsShard.offset());
        assertWithMessage("FIXTURE: one container must leave its shard while keeping its queue entry, or this "
                + "arm has nothing to distinguish collected from left alone")
                .that(removedByHand)
                .isSameInstanceAs(willLeaveItsShard);

        wm.onPartitionsRevoked(UniLists.of(residentPartition, departedPartition));

        assertWithMessage("THE PRODUCTION SWEEP DOES NOT TOUCH THE RETRY QUEUE. If this has gone red, a "
                + "rebalance callback is reaching RetryQueue again - which is an unbounded, fair write-lock "
                + "acquire on the broker-poll thread inside poll(), and ArchitectureTest."
                + "rebalanceCallbacksMustNotBlock should be red beside it")
                .that(wm.getSm().getRetryQueue().contains(stillResident)
                        && wm.getSm().getRetryQueue().contains(willLeaveItsShard))
                .isTrue();

        var ignoredWork = wm.getWorkIfAvailable(10);

        assertWithMessage("the controller's purge collects the entry whose container left its shard")
                .that(wm.getSm().getRetryQueue().contains(willLeaveItsShard))
                .isFalse();
        assertWithMessage("CONTROL: and the one whose container the sweep DID find is gone with it - both "
                + "partitions were revoked, so by now neither container is resident anywhere")
                .that(wm.getSm().getRetryQueue().contains(stillResident))
                .isFalse();
    }

    /**
     * P2 - control arm, same magnitude one seam EARLIER. The identical rebalance fired at staleness
     * checkpoint 3's lookup instead of at the live re-validation: {@code onFailureResult}'s live check then
     * sees the incremented epoch and drops the re-queue, which is astubbs#346's fix doing its job.
     * <p>
     * Green on master, and that is the point: it isolates the window above to the gap between the live check
     * and the add, rather than to "a rebalance during a failure result" in general.
     */
    @Test
    void theSameRebalanceOneSeamEarlierIsCaughtByTheLiveCheck() {
        WorkContainer<String, String> wc = aFailedRecordTakenAsWork();

        // NOT armed on the live-check seam - fired before handleFutureResult reaches onFailureResult at all,
        // which is where checkpoint 3 sits
        wm.onPartitionsRevoked(UniLists.of(tp));
        wm.handleFutureResult(wc);

        assertWithMessage("control arm: the live-check seam must NOT have been armed")
                .that(wm.raceHasFired())
                .isFalse();

        assertWithMessage("serialised, the live check answers stale and the re-queue is skipped")
                .that(wm.getSm().getRetryQueue().contains(wc))
                .isFalse();

        module.getMutableClock().add(Duration.ofHours(1));

        assertWithMessage("serialised, nothing reads as waiting to be processed")
                .that(wm.isRecordsAwaitingProcessing())
                .isFalse();
    }

    /**
     * P3 - control arm, same magnitude one seam LATER. The rebalance completes wholly AFTER the re-queue, so
     * the sweep finds the container in its shard and takes it out.
     * <p>
     * <b>Re-stated when the sweep stopped touching the retry queue.</b> It used to remove the paired queue
     * entry itself, and this arm asserted that. It cannot any more: the sweep runs on the broker-poll thread
     * inside {@code poll()} and the queue's write lock is unbounded and fair, so the callback removes from the
     * shards alone and {@link ShardManager#purgeDepartedRetryEntries()} collects the entry on the controller
     * thread. What the arm asserts is therefore in two steps rather than one, and the second step is the
     * bound: one control-loop pass.
     */
    @Test
    void theSameRebalanceOneSeamLaterLeavesTheQueueEntryForTheControllerToCollect() {
        WorkContainer<String, String> wc = aFailedRecordTakenAsWork();

        wm.handleFutureResult(wc);

        assertWithMessage("PRECONDITION: with no rebalance yet, the failed record must be parked for retry - "
                + "otherwise the sweep below has nothing to prove")
                .that(wm.getSm().getRetryQueue().contains(wc))
                .isTrue();

        wm.onPartitionsRevoked(UniLists.of(tp));

        assertWithMessage("the sweep takes the shard entry - that part is unchanged, and it is all a rebalance "
                + "callback is allowed to do")
                .that(wm.getSm().getNumberOfRecordsInShards())
                .isEqualTo(0L);
        assertWithMessage("and it leaves the queue entry alone rather than waiting for the write lock")
                .that(wm.getSm().getRetryQueue().contains(wc))
                .isTrue();

        var ignoredWork = wm.getWorkIfAvailable(10);

        assertWithMessage("one controller pass collects it - the departed entry's whole lifetime")
                .that(wm.getSm().getRetryQueue().contains(wc))
                .isFalse();
    }
}
