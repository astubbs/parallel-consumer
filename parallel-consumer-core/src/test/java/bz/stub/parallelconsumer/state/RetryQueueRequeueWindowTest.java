package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

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

    /**
     * A {@link ProcessingShard} that runs the controller's action at one exact instruction INSIDE the production
     * revoke sweep: after the sweep's first retry-queue removal, and before the shard removal that follows it.
     * <p>
     * <b>Why a shard and not another {@link RacingSeamWorkManager} seam.</b> The seams that class provides sit
     * on the CONTROLLER's side, which is the right place for an add that arrives after a sweep has finished.
     * The window this catches is the other nesting - the controller's re-queue landing inside the sweep - and
     * the only production instruction between the sweep's two queue removals belongs to the shard.
     * {@link ProcessingShard#removeWorkAtOffsetPairedWith} is that instruction.
     * <p>
     * <b>One shot</b>, for the same reason {@link RacingSeamWorkManager} gives: the revoke path reaches this
     * method more than once (the epoch-change sweep follows the revoke sweep) and re-firing a whole re-queue on
     * each pass models nothing real. Firing is tracked in its own flag rather than inferred from the armed slot
     * being clear, so a precondition assertion cannot pass on an arm that forgot to arm.
     */
    static class SeamShard extends ProcessingShard<String, String> {

        private Runnable interference;

        private boolean raceFired;

        SeamShard(ShardKey key, ParallelConsumerOptions<?, ?> options, PartitionStateManager<String, String> pm,
                  RecordPopulation population, DispatchScanMeter scanMeter) {
            super(key, options, pm, population, scanMeter);
        }

        void arm(Runnable interference) {
            this.interference = interference;
        }

        boolean raceHasFired() {
            return raceFired;
        }

        @Override
        WorkContainer<String, String> removeWorkAtOffsetPairedWith(
                long offset, Predicate<WorkContainer<String, String>> pairedQueueRemoval) {
            if (interference != null) {
                Runnable armed = interference;
                interference = null;
                raceFired = true;
                armed.run();
            }
            return super.removeWorkAtOffsetPairedWith(offset, pairedQueueRemoval);
        }
    }

    /**
     * Installs a {@link SeamShard} as the real shard for {@code partition}, so the production sweep runs through
     * it.
     * <p>
     * It has to be planted BEFORE any record arrives: {@code ShardManager.addWorkContainer} constructs a plain
     * {@link ProcessingShard} only when the map has none for the key, so an already-present one is what
     * production then uses and writes into. The shard is built with the manager's own {@link RecordPopulation},
     * not a fresh one, or {@code getNumberOfRecordsInShards()} would not see anything this shard holds.
     */
    private SeamShard plantASeamShardFor(TopicPartition partition) {
        Map<ShardKey, ProcessingShard<String, String>> shards = new ConcurrentHashMap<>();
        wm.getSm().setProcessingShards(shards);

        var anyRecordOnThatPartition = new ConsumerRecord<>(partition.topic(), partition.partition(), 0L, "k", "v");
        var key = ShardKey.of(anyRecordOnThatPartition, module.options().getOrdering());
        // the manager's OWN meter, not a fresh one: it is shared across every shard of one ShardManager, and a
        // planted shard that counted into its own would silently drop what production examined through it
        var seamShard = new SeamShard(key, module.options(), wm.getPm(), wm.getSm().getRecordPopulation(),
                wm.getSm().getDispatchScanMeter());
        shards.put(key, seamShard);
        return seamShard;
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
     * P1, second half - the orphan is not merely present but unremovable. Every route that takes an entry out
     * of the retry queue reaches it through shard contents, and the shard has none.
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
     * @param queueFirst           the order the sweep does its two removals in: {@code true} models the
     *                             declining sweep's ordering, {@code false} models the shard-first one it
     *                             replaced
     * @param pairTheSecondRemoval whether the sweep repeats its queue removal after the shard removal, which is
     *                             what production does and what the queue-first ordering needs
     */
    private void sweepAroundTheRequeue(WorkContainer<String, String> wc, boolean queueFirst,
                                       boolean pairTheSecondRemoval) {
        var shard = wm.getSm().getShard(wm.getSm().computeShardKey(wc)).get();
        Runnable queueRemoval = () -> {
            // Named rather than discarded, and deliberately NOT asserted: whether an entry was present depends
            // on which position this runnable is in. Queue-first finds nothing, because the controller has not
            // added yet - that IS astubbs/parallel-consumer#431's ordering and the reason its arm makes an
            // orphan. Shard-first finds the controller's add. Both are the modelled behaviour, so the value is
            // recorded to show it was considered rather than dropped.
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
        if (pairTheSecondRemoval) {
            // production's second ask - see ShardManager.removeWorkFromShardFor. Modelled as the same runnable
            // run again, because that is exactly what it is: the same removal by the same coordinates, made once
            // the container has left the shard.
            queueRemoval.run();
        }
    }

    /**
     * <b>The control for the arm below: the same queue-first ordering with the sweep's SECOND queue removal left
     * out.</b> {@link ShardManager#onFailure} owns why one confirmation is not enough against that ordering;
     * this arm is the demonstration of it.
     * <p>
     * <b>It asserts the orphan APPEARS, and it describes a shape production does not have.</b> Delete the
     * repeated removal and this is what is left, which is the whole reason the repeat exists. Same magnitude,
     * one term changed, against {@link #aQueueFirstSweepWithItsPairedSecondRemovalKeepsThePairWhole()}.
     * <p>
     * <b>It cannot detect production changing under it, and saying it could was the over-claim review caught on
     * astubbs/parallel-consumer#437.</b> It hand-builds the ordering, so nothing here moves when the sweep does.
     * {@link #theProductionSweepTakesOutAnEntryTheControllerAddedInsideTheSweep()} is what goes red then.
     */
    @Test
    void aQueueFirstSweepWithoutItsPairedSecondRemovalWouldOrphanTheEntry() {
        WorkContainer<String, String> wc = aFailedRecordTakenAsWork();

        sweepAroundTheRequeue(wc, true, false);

        assertWithMessage("PRECONDITION: the container must have left its shard, or there is no orphan to make")
                .that(wm.getSm().getNumberOfRecordsInShards())
                .isEqualTo(0L);

        assertWithMessage("CONTROL: with the sweep removing from the QUEUE first and NOT repeating that removal "
                + "afterwards, the residency read still sees a resident container, the add is not undone, and "
                + "the entry is orphaned. If this has gone green, either the confirmation has started catching "
                + "this half on its own - which the argument on ShardManager.onFailure says it cannot - or this "
                + "arm has stopped modelling the ordering it names")
                .that(wm.getSm().getRetryQueue().contains(wc))
                .isTrue();
    }

    /**
     * <b>Production's shape: queue-first, with the queue removal repeated after the shard removal.</b> The
     * inversion of the control above - one term changed, the second removal - and the half of
     * {@link ShardManager#onFailure} its residency confirmation cannot reach. That method owns why the two
     * halves are each necessary.
     */
    @Test
    void aQueueFirstSweepWithItsPairedSecondRemovalKeepsThePairWhole() {
        WorkContainer<String, String> wc = aFailedRecordTakenAsWork();

        sweepAroundTheRequeue(wc, true, true);

        assertWithMessage("PRECONDITION: the container must have left its shard")
                .that(wm.getSm().getNumberOfRecordsInShards())
                .isEqualTo(0L);

        assertWithMessage("the sweep's SECOND queue removal must take out the entry the controller added "
                + "between the sweep's two removals - otherwise it is a queue-only orphan")
                .that(wm.getSm().getRetryQueue().contains(wc))
                .isFalse();
    }

    /**
     * The matched control for the ordering itself: the identical steps with only the sweep's internal order
     * changed, and no second removal. Shard-first keeps the pair whole on the confirmation alone - the ordering
     * master had before astubbs/parallel-consumer#431.
     * <p>
     * Read with the two arms above the pair of terms is complete: the ordering decides whether the confirmation
     * can see the departure, and the second removal is what replaces it when it cannot.
     */
    @Test
    void aShardFirstSweepIsCaughtByTheConfirmation() {
        WorkContainer<String, String> wc = aFailedRecordTakenAsWork();

        sweepAroundTheRequeue(wc, false, false);

        assertWithMessage("PRECONDITION: the container must have left its shard")
                .that(wm.getSm().getNumberOfRecordsInShards())
                .isEqualTo(0L);

        assertWithMessage("with the sweep removing from the SHARD first, the residency read after the add sees "
                + "a departed container and takes the entry back out")
                .that(wm.getSm().getRetryQueue().contains(wc))
                .isFalse();
    }

    /**
     * <b>The mechanical coupling between the modelled arms above and the production code they model.</b>
     * <p>
     * Those arms hand-build the sweep's removals, so none of them can fail when production's ordering changes -
     * which on its own leaves the promised regression signal resting on a future author remembering to rewrite a
     * test. Raised in review on astubbs/parallel-consumer#437, and correct. This arm supplies what they cannot:
     * it drives the REAL revoke sweep ({@code WorkManager#onPartitionsRevoked} ->
     * {@code ShardManager.removeWorkFromShardFor}) and pins the ordering the abandon-on-refusal design needs -
     * <b>that the queue is asked BEFORE the shard is touched</b>, so that a refusal can abandon a removal that
     * has not started yet.
     * <p>
     * The container under test has already left its shard when the sweep runs, so a shard-first sweep would
     * never reach its queue entry and a queue-first one takes it out regardless. The resident container is the
     * arm's own control: without it, "the departed container's entry went" would also pass on a sweep that
     * removed everything for the wrong reason.
     */
    @Test
    void theProductionSweepAsksTheQueueBeforeItTouchesTheShard() {
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
                + "arm has nothing to distinguish an unconditional ask from a gated one")
                .that(removedByHand)
                .isSameInstanceAs(willLeaveItsShard);

        wm.onPartitionsRevoked(UniLists.of(residentPartition, departedPartition));

        assertWithMessage("CONTROL: the sweep must remove the entry of a container it DID find in its shard - "
                + "without this the assertion below would also pass on a sweep that never ran")
                .that(wm.getSm().getRetryQueue().contains(stillResident))
                .isFalse();

        assertWithMessage("PRODUCTION ASKS THE QUEUE FIRST, and abandoning a refused removal depends on it: "
                + "nothing has been taken out of the shard yet when the answer arrives, so a refusal can leave "
                + "the pair untouched. An entry whose container had already left its shard therefore goes too. "
                + "If this has gone red, production now gates its queue removal on the shard removal - the "
                + "shard-first ordering aShardFirstSweepIsCaughtByTheConfirmation models, where a refusal has "
                + "already split the pair by the time it is refused")
                .that(wm.getSm().getRetryQueue().contains(willLeaveItsShard))
                .isFalse();
    }

    /**
     * <b>The other half, and the one no hand-built arm can reach: the controller's re-queue landing INSIDE the
     * production sweep, between its two queue removals.</b>
     * <p>
     * {@link SeamShard} runs {@code sm.onFailure} at the one instruction where that interleaving is decided.
     * Only the sweep's SECOND queue removal can catch it, and this arm asserts it does - <b>red without that
     * removal</b>, which is how it was checked.
     */
    @Test
    void theProductionSweepTakesOutAnEntryTheControllerAddedInsideTheSweep() {
        var seamShard = plantASeamShardFor(tp);
        var wc = ModelUtils.registerOneRecordAndTakeIt(wm, tp);
        wc.onUserFunctionFailure(new RuntimeException("simulated user function failure"));

        assertWithMessage("FIXTURE: the queue must be empty at the sweep's FIRST ask, or that ask removes the "
                + "entry and this arm says nothing about the second one")
                .that(wm.getSm().getRetryQueue().contains(wc))
                .isFalse();

        var theAddStood = new AtomicBoolean();
        seamShard.arm(() -> {
            wm.getSm().onFailure(wc);
            theAddStood.set(wm.getSm().getRetryQueue().contains(wc));
        });

        wm.onPartitionsRevoked(UniLists.of(tp));

        assertWithMessage("PRECONDITION: the armed re-queue must have fired inside the sweep - without it this "
                + "arm drives no window at all")
                .that(seamShard.raceHasFired())
                .isTrue();
        assertWithMessage("PRECONDITION: the controller's add must have STOOD - the residency read it makes "
                + "sees a resident container at that instant, so this is the state the second removal is for. "
                + "If the confirmation had undone it, this arm would be asserting nothing")
                .that(theAddStood.get())
                .isTrue();
        assertWithMessage("PRECONDITION: the container must have left its shard")
                .that(wm.getSm().getNumberOfRecordsInShards())
                .isEqualTo(0L);

        assertWithMessage("the sweep's SECOND queue removal must take out the entry the controller added "
                + "between the sweep's two removals; without it that entry is a queue-only orphan, held by "
                + "nothing that any scan reaches, and it keeps a draining close open to its timeout")
                .that(wm.getSm().getRetryQueue().contains(wc))
                .isFalse();
    }

    /**
     * <b>The refused second removal, and the put-back that keeps the pair whole.</b>
     * <p>
     * Same seam as the arm above with one term added: the interference opens a live {@link RetryQueue} iterator
     * and leaves it open, so the read lock is held when the sweep asks the second time and {@code tryLock()}
     * refuses. A {@code ReentrantReadWriteLock} grants no upgrade, so a thread holding the read lock is refused
     * the write lock even though it is the same thread - which is what makes this deterministic without a second
     * thread. The {@code add} is made BEFORE the iterator is opened, because it takes the write lock and would
     * otherwise deadlock against the reader on this thread.
     * <p>
     * All three parts of the pair are asserted: resident again, entry still there, selection claim back with it.
     * <b>Red without the put-back</b>, which is how it was checked.
     */
    @Test
    void aRefusedSecondRemovalPutsTheContainerBackSoThePairStaysWhole() {
        var seamShard = plantASeamShardFor(tp);
        var wc = ModelUtils.registerOneRecordAndTakeIt(wm, tp);
        wc.onUserFunctionFailure(new RuntimeException("simulated user function failure"));

        var readLockHeldThroughTheSweep = new AtomicReference<RetryQueue.RetryQueueIterator>();
        seamShard.arm(() -> {
            wm.getSm().onFailure(wc);
            readLockHeldThroughTheSweep.set(wm.getSm().getRetryQueue().iterator());
        });

        try {
            wm.onPartitionsRevoked(UniLists.of(tp));
        } finally {
            var stillOpen = readLockHeldThroughTheSweep.get();
            if (stillOpen != null) {
                stillOpen.close();
            }
        }

        assertWithMessage("PRECONDITION: the armed interference must have fired inside the sweep")
                .that(seamShard.raceHasFired())
                .isTrue();
        assertWithMessage("PRECONDITION: the read lock must actually have been taken, or the second removal "
                + "was never refused and this arm exercises no put-back")
                .that(readLockHeldThroughTheSweep.get())
                .isNotNull();

        assertWithMessage("the refused second removal must put the container BACK in its shard - leaving it out "
                + "is exactly the queue-only orphan the second removal exists to prevent")
                .that(seamShard.getWorkContainerAtOffset(wc.offset()).orElse(null))
                .isSameInstanceAs(wc);
        assertWithMessage("the entry stays too, so what is left is a WHOLE stale pair: the engine tolerates a "
                + "stale resident and the controller's own sweep retires both halves, which it cannot do to an "
                + "orphan")
                .that(wm.getSm().getRetryQueue().contains(wc))
                .isTrue();
        assertWithMessage("the population must be conserved by the put-back - RecordPopulation has no clamp, so "
                + "a retirement that is not matched by a re-admission is a permanent deficit")
                .that(wm.getSm().getNumberOfRecordsInShards())
                .isEqualTo(1L);
        assertWithMessage("and so must the selection claim: the container was awaiting selection when the sweep "
                + "took it, so it must be awaiting selection again now it is back. A put-back that skipped this "
                + "would under-report work for as long as the container stayed")
                .that(seamShard.getCountOfWorkAwaitingSelection())
                .isEqualTo(1L);
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
     * the sweep finds the container in its shard, and {@code removeWorkFromShardFor} takes the paired retry
     * queue entry with it. Green on master: the pair only splits when the rebalance lands in the window.
     */
    @Test
    void theSameRebalanceOneSeamLaterTakesTheQueueEntryWithTheShardEntry() {
        WorkContainer<String, String> wc = aFailedRecordTakenAsWork();

        wm.handleFutureResult(wc);

        assertWithMessage("PRECONDITION: with no rebalance yet, the failed record must be parked for retry - "
                + "otherwise the sweep below has nothing to prove")
                .that(wm.getSm().getRetryQueue().contains(wc))
                .isTrue();

        wm.onPartitionsRevoked(UniLists.of(tp));

        assertWithMessage("the sweep reaches the queue entry through the shard entry, and removes both")
                .that(wm.getSm().getRetryQueue().contains(wc))
                .isFalse();
    }
}
