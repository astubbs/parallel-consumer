package bz.stub.parallelconsumer.state;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.internal.DynamicLoadFactor;
import bz.stub.parallelconsumer.internal.EpochAndRecordsMap;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;

import java.time.Duration;
import java.util.List;

import static com.google.common.truth.Truth.assertWithMessage;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.PARTITION;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * Reproduces candidate 3 of the torn-read family dossier: {@code WorkManager.handleFutureResult}'s staleness
 * checkpoint (checkpoint 3 of the scheme documented at {@code PartitionState#epochIsStale}) and its acting
 * branches read {@code partitionStates.get(tp)} on separate lookups. The check passes against the OLD state;
 * a rebalance completing on the broker-poll thread in the gap swaps the map entry; the action then runs against
 * the swapped state. This contradicts checkpoint 3's documented guarantee ("work that went stale mid-flight
 * never reaches onSuccessResult/onFailureResult").
 * <p>
 * <b>The forced race.</b> {@code RacingStaleCheckWorkManager} overrides {@link WorkManager#checkIfWorkIsStale},
 * computes the real answer first, then fires the full production rebalance
 * ({@code onPartitionsRevoked}, optionally followed by {@code onPartitionsAssigned}) before returning it - the
 * torn read, deterministic.
 * <p>
 * Two traced harms, one test each; <b>both tests are EXPECTED RED on master</b> (this branch carries
 * reproductions, not fixes):
 * <ul>
 * <li><b>Failure path</b>: {@code onFailureResult -> sm.onFailure} re-adds the stale container to the retry
 * queue. Under PARTITION/UNORDERED ordering the revoked partition's shard object survives its own sweep (only
 * KEY ordering removes empty shards), so the shard-present guard passes and the add goes through. Nothing can
 * ever remove it: {@code removeStaleContainers} only cleans retry-queue entries it finds via shard contents, and
 * the sweep already emptied the shard. The orphan permanently inflates the ready-to-retry count that feeds
 * {@code workIsWaitingToBeProcessed} and the poller gate - the same consequence family as
 * docs/inflight/bug-retry-queue-orphaned-by-inline-stale-removal.md, reached by a different door.</li>
 * <li><b>Success path</b> (revoke + reassign in the gap): {@code pm.onSuccess} acts on the freshly assigned
 * state. The offset is absent from the fresh state's incompletes, so {@code PartitionState.onSuccess}'s
 * {@code assert removedFromIncompletes} fires under {@code -ea} - an {@link AssertionError} out of the control
 * loop. Without {@code -ea} the failure is silent and worse: the fresh, still-bootstrap-phase state is marked
 * dirty and its high-water mark raised by a completion from a dead epoch - which is precisely the gate-opener
 * that makes the bootstrap-reset tear (candidate 1, {@link PartitionStateBootstrapResetTearTest}) reachable.</li>
 * </ul>
 *
 * @author Antony Stubbs
 */
@Slf4j
class WorkManagerStaleCheckDoubleLookupTest {

    static final String TOPIC = "myTopic";
    final TopicPartition tp = new TopicPartition(TOPIC, 0);

    /**
     * A {@link WorkManager} that completes a full rebalance in the gap between checkpoint 3's staleness lookup
     * and the acting lookups. Firing is tracked in an explicit {@code raceFired} boolean, set at firing time: a
     * cleared armed-slot cannot tell "armed, then fired" from "never armed", so a guard built on it would pass on
     * a test that forgot to arm.
     */
    static class RacingStaleCheckWorkManager extends WorkManager<String, String> {
        private transient Runnable interference;
        private boolean raceFired;

        RacingStaleCheckWorkManager(PCModuleTestEnv module) {
            super(module, new DynamicLoadFactor(2, 4));
        }

        void arm(Runnable interference) {
            this.interference = interference;
        }

        boolean raceHasFired() {
            return raceFired;
        }

        @Override
        public boolean checkIfWorkIsStale(WorkContainer<String, String> workContainer) {
            boolean staleAnswerFromFirstLookup = super.checkIfWorkIsStale(workContainer);
            if (interference != null) {
                Runnable oneShot = interference;
                interference = null;
                raceFired = true;
                oneShot.run();
            }
            return staleAnswerFromFirstLookup;
        }
    }

    final PCModuleTestEnv module = new PCModuleTestEnv(ParallelConsumerOptions.<String, String>builder()
            .ordering(PARTITION)
            .consumer(new MockConsumer<>(OffsetResetStrategy.EARLIEST))
            .build());

    final RacingStaleCheckWorkManager wm = new RacingStaleCheckWorkManager(module);

    {
        // install the racing double before anything asks the module for a work manager
        module.setWorkManager(wm);
    }

    private WorkContainer<String, String> registerOneRecordAndTakeIt() {
        wm.onPartitionsAssigned(UniLists.of(tp));

        var record = new ConsumerRecord<>(TOPIC, tp.partition(), 0, "key-0", "value");
        var records = new ConsumerRecords<>(UniMaps.of(tp, UniLists.of(record)));
        wm.registerWork(new EpochAndRecordsMap<>(records, wm.getPm()));

        List<WorkContainer<String, String>> taken = wm.getWorkIfAvailable();
        assertWithMessage("fixture: exactly the one registered container must be selectable")
                .that(taken).hasSize(1);
        return taken.get(0);
    }

    /**
     * Failure path: the rebalance lands between the staleness check and the acting reads, and the stale failed
     * container is re-queued for retry on a partition this consumer no longer owns. RED on master: the container
     * is in the retry queue, the stale sweep cannot find it (its shard entry is already gone), and after its
     * retry delay elapses it counts as ready-to-retry forever - the permanent-orphan consequence.
     */
    @Test
    void staleFailureResultMustNotBePermanentlyRequeued() {
        WorkContainer<String, String> wc = registerOneRecordAndTakeIt();
        wc.onUserFunctionFailure(new RuntimeException("simulated user function failure"));

        wm.arm(() -> wm.onPartitionsRevoked(UniLists.of(tp)));
        wm.handleFutureResult(wc);

        assertWithMessage("the armed rebalance must actually have fired inside the gap")
                .that(wm.raceHasFired())
                .isTrue();

        // the sweep that exists to clean stale containers out of both structures
        wm.getSm().removeStaleContainers();

        assertWithMessage("checkpoint 3's contract: a result from a dead epoch never reaches "
                + "onFailureResult, so nothing from the revoked partition may survive in the retry queue - "
                + "and this entry is unremovable, because the revoke sweep already emptied its shard, which "
                + "is the only route removeStaleContainers has to it")
                .that(wm.getSm().getRetryQueue().contains(wc))
                .isFalse();

        // let the orphan's retry delay pass - from here on it reads as ready-to-retry, forever
        module.getMutableClock().add(Duration.ofHours(1));

        assertWithMessage("nothing is assigned and nothing is in flight, so no work can be waiting - a true "
                + "here is the permanently inflated ready-to-retry count that gates the broker poller")
                .that(wm.workIsWaitingToBeProcessed())
                .isFalse();
    }

    /**
     * Success path, with the partition reassigned in the same gap: the acting lookup returns the FRESH state,
     * whose incompletes do not contain the offset. RED on master: {@code PartitionState.onSuccess}'s
     * {@code assert removedFromIncompletes} throws under {@code -ea} (surefire enables assertions), out through
     * {@code handleFutureResult} - in production, out of the control loop. Without {@code -ea} the same tear
     * silently dirties a bootstrap-phase state with a dead epoch's completion - the candidate-1 gate opener.
     */
    @Test
    void staleSuccessResultMustNotActOnTheFreshlyAssignedState() {
        WorkContainer<String, String> wc = registerOneRecordAndTakeIt();
        wc.onUserFunctionSuccess();

        wm.arm(() -> {
            wm.onPartitionsRevoked(UniLists.of(tp));
            wm.onPartitionsAssigned(UniLists.of(tp));
        });

        assertDoesNotThrow(() -> wm.handleFutureResult(wc),
                "candidate 3: a success result from a dead epoch must be dropped at checkpoint 3, not applied "
                        + "to the freshly assigned partition state");

        assertWithMessage("the armed rebalance must actually have fired inside the gap")
                .that(wm.raceHasFired())
                .isTrue();

        assertWithMessage("the fresh state is in its bootstrap phase and has completed nothing of its own, so "
                + "it must not be dirty - a dirty bootstrap state is exactly what re-opens the bootstrap-reset "
                + "tear's commit window (candidate 1)")
                .that(wm.getPm().getPartitionState(tp).isDirty())
                .isFalse();
    }

    /**
     * Control arm for the failure path: the identical rebalance, completed wholly BEFORE
     * {@code handleFutureResult} instead of inside the gap. Checkpoint 3 then sees the stale epoch and drops the
     * result. Green on master: same mutation, different position, so the orphan above is the interleaving's.
     */
    @Test
    void rebalanceCompletedBeforeTheFailureResultIsDroppedAtCheckpointThree() {
        WorkContainer<String, String> wc = registerOneRecordAndTakeIt();
        wc.onUserFunctionFailure(new RuntimeException("simulated user function failure"));

        wm.onPartitionsRevoked(UniLists.of(tp));
        wm.handleFutureResult(wc);
        wm.getSm().removeStaleContainers();

        assertWithMessage("control arm must not have armed the race")
                .that(wm.raceHasFired())
                .isFalse();

        assertWithMessage("serialized, checkpoint 3 drops the stale failure and nothing is re-queued")
                .that(wm.getSm().getRetryQueue().contains(wc))
                .isFalse();

        module.getMutableClock().add(Duration.ofHours(1));

        assertWithMessage("serialized, nothing reads as waiting to be processed")
                .that(wm.workIsWaitingToBeProcessed())
                .isFalse();
    }

    /**
     * Control arm for the success path: revoke and reassign complete wholly before the result arrives.
     * Checkpoint 3 drops it against the fresh state's epoch; nothing throws, nothing is dirtied. Green on master.
     */
    @Test
    void rebalanceCompletedBeforeTheSuccessResultIsDroppedAtCheckpointThree() {
        WorkContainer<String, String> wc = registerOneRecordAndTakeIt();
        wc.onUserFunctionSuccess();

        wm.onPartitionsRevoked(UniLists.of(tp));
        wm.onPartitionsAssigned(UniLists.of(tp));

        assertDoesNotThrow(() -> wm.handleFutureResult(wc),
                "serialized, checkpoint 3 drops the stale success cleanly");

        assertWithMessage("control arm must not have armed the race")
                .that(wm.raceHasFired())
                .isFalse();

        assertWithMessage("serialized, the fresh state stays untouched")
                .that(wm.getPm().getPartitionState(tp).isDirty())
                .isFalse();
    }
}
