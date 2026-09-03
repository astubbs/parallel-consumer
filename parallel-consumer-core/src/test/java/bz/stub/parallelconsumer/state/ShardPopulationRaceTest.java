package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.internal.utils.ThreadUtils;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static java.util.Objects.requireNonNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

/**
 * The conservation figure the broker-poller load gate reads is {@code admitted - retired}, and nothing
 * reconciles it against the shards it describes - so every drift is permanent and silent. These are the
 * interleavings that produce one, each driven deterministically rather than raced for.
 * <p>
 * Two of the three seams are placed by making a collaborator do something on the way past, which is what lets
 * a single-threaded test land another thread's action at an exact instruction:
 * {@link PartitionStateManager#getPartitionState(WorkContainer)} for the two inside
 * {@link ProcessingShard#addWorkContainer} and {@link ProcessingShard#removeStaleWorkContainersFromShard},
 * and {@link ConsumerRecord#offset()} for the one between {@link ShardManager} choosing a shard and writing
 * to it. The seam is a stand-in for the other thread, not a fixture the product knows about; if any of these
 * calls stops happening where the test assumes, the test stops exercising its window and must be rewritten
 * rather than left silently passing - each one asserts its own precondition for that reason.
 *
 * @author Antony Stubbs
 * @see RecordPopulation
 * @see ProcessingShard
 */
class ShardPopulationRaceTest {

    private static final String TOPIC = "topic";

    private static final TopicPartition TP = new TopicPartition(TOPIC, 0);

    private final ModelUtils mu = new ModelUtils();

    private final PCModuleTestEnv module = mu.getModule();

    private final WorkManager<String, String> wm = module.workManager();

    /**
     * An insertion that only <em>looked</em> like a replacement still has to be admitted.
     * <p>
     * {@code addWorkContainer} reads the resident container, decides it is stale, and replaces it. Between
     * that read and the insertion the poller's stale sweep can remove the resident and retire it - so the
     * container being inserted is the only thing at the offset, not a replacement for anything. Skipping the
     * admission there leaves the shard holding a record the population does not know about, while its
     * eventual departure retires anyway: the figure sits permanently below the truth, under-throttles, and
     * over-fetches from the broker for the life of the consumer.
     */
    @Test
    void anInsertionThatOnlyLookedLikeAReplacementIsStillAdmitted() {
        var population = new RecordPopulation();
        var seam = spy(wm.getPm());
        var record = recordAt(10);
        var shard = shardWith(seam, population, record);

        wm.onPartitionsAssigned(UniLists.of(TP));
        long firstEpoch = wm.getPm().getEpochOfPartition(TP);
        shard.addWorkContainer(new WorkContainer<>(firstEpoch, record, module));
        assertThat(population.getInSystem()).isEqualTo(1L);

        // the partition is taken away and handed back, so what the shard is still holding is now stale
        wm.onPartitionsRevoked(UniLists.of(TP));
        wm.onPartitionsAssigned(UniLists.of(TP));
        long laterEpoch = wm.getPm().getEpochOfPartition(TP);
        assertWithMessage("PRECONDITION: the rebalance must actually have made the resident stale")
                .that(laterEpoch).isGreaterThan(firstEpoch);

        // THE INTERLEAVING: the sweep lands between the read of the resident and the insertion
        onNextStalenessCheck(seam, () -> shard.removeStaleWorkContainersFromShard(new RetryQueue()));

        shard.addWorkContainer(new WorkContainer<>(laterEpoch, record, module));

        assertWithMessage("PRECONDITION: the sweep must have run inside the add, retiring the resident")
                .that(population.getRetiredTotal()).isEqualTo(1L);
        assertThat(shard.getCountOfWorkTracked()).isEqualTo(1L);
        assertWithMessage("the fresh container is the only thing at this offset, so it was an insertion and "
                + "has to be admitted - an admission skipped here is a deficit nothing ever repairs")
                .that(population.getInSystem()).isEqualTo(1L);
    }

    /**
     * A sweep that removed nothing must not retire.
     * <p>
     * The revocation sweep and the stale sweep run on different threads and can reach the same offset. Only
     * one of them removes anything, but both used to retire, so a single admission was retired twice - and
     * {@link RecordPopulation} has no clamp, so the population goes below what the shards hold and stays
     * there.
     */
    @Test
    void aSweepThatRemovedNothingMustNotRetireTheRecordAgain() {
        var population = new RecordPopulation();
        var seam = spy(wm.getPm());
        var record = recordAt(20);
        var shard = shardWith(seam, population, record);

        wm.onPartitionsAssigned(UniLists.of(TP));
        shard.addWorkContainer(new WorkContainer<>(wm.getPm().getEpochOfPartition(TP), record, module));

        // the partition goes away, so the resident is stale AND the revocation sweep is about to reach it
        wm.onPartitionsRevoked(UniLists.of(TP));

        // THE INTERLEAVING: the revocation sweep removes and retires the container while the stale sweep is
        // between yielding it and removing it, so the stale sweep's own removal takes nothing out
        onNextStalenessCheck(seam, () -> shard.removeWorkAtOffset(20L));

        var swept = shard.removeStaleWorkContainersFromShard(new RetryQueue());

        assertThat(shard.getCountOfWorkTracked()).isEqualTo(0L);
        assertWithMessage("one admission, retired exactly once - by whichever call actually removed it")
                .that(population.getRetiredTotal()).isEqualTo(population.getAdmittedTotal());
        assertWithMessage("so the population lands on zero rather than below it")
                .that(population.getInSystem()).isEqualTo(0L);
        assertWithMessage("and the available counter is not deducted twice either")
                .that(shard.getCountOfWorkAwaitingSelection()).isEqualTo(0L);
        assertWithMessage("the revocation sweep won, so this sweep reports nothing - a container it did not "
                + "remove is not its to hand to the retry-queue cleanup either")
                .that(swept).isEmpty();
    }

    /**
     * {@link ProcessingShard#getWorkIfAvailable}'s last-resort stale eviction has to clean the retry queue as
     * well as the shard, the way {@link ShardManager#removeStaleContainers()} does.
     * <p>
     * A queue entry whose container is no longer in any shard is removed by nothing, ever. The workable
     * figure the load gate reads subtracts the parked-for-retry count from the population, so an orphan is
     * subtracted from a total that no longer contains it and the poller is told it holds less than it does.
     */
    @Test
    void theInlineStaleSweepTakesTheRecordOutOfTheRetryQueueToo() {
        var population = new RecordPopulation();
        var retryQueue = new RetryQueue();
        var record = recordAt(30);
        var shard = shardWith(wm.getPm(), population, record);

        wm.onPartitionsAssigned(UniLists.of(TP));
        var container = new WorkContainer<>(wm.getPm().getEpochOfPartition(TP), record, module);
        shard.addWorkContainer(container);

        // taken as work, failed, and parked in the retry queue - the only state in which the inline
        // eviction can orphan a queue entry
        assertThat(shard.getWorkIfAvailable(10, retryQueue)).containsExactly(container);
        container.onUserFunctionFailure(new RuntimeException("deliberate"));
        container.endFlight();
        shard.onFailure(container);
        retryQueue.add(container);
        assertWithMessage("PRECONDITION: the failed record is parked in the retry queue")
                .that(retryQueue.contains(container)).isTrue();

        // the partition goes away, and the inline eviction is what reaches the container - not
        // ShardManager.removeStaleContainers(), which cleans both structures
        wm.onPartitionsRevoked(UniLists.of(TP));

        assertThat(shard.getWorkIfAvailable(10, retryQueue)).isEmpty();

        assertThat(shard.getCountOfWorkTracked()).isEqualTo(0L);
        assertThat(population.getInSystem()).isEqualTo(0L);
        assertWithMessage("the queue entry goes with it - nothing else will ever remove one whose container "
                + "is no longer held by any shard")
                .that(retryQueue.contains(container)).isFalse();
    }

    /**
     * A record must never be admitted into a shard that has been garbage-collected.
     * <p>
     * Under {@link ParallelConsumerOptions.ProcessingOrder#KEY}, {@code removeShardIfEmpty} collects a shard
     * the moment it is empty. Choosing the shard and writing to it therefore have to be one step: a shard
     * handed back and then dropped leaves the record in a map no scan reaches, and - unlike the old gate,
     * which summed only the shards still registered - a conservation figure cannot forget the admission. It
     * reads permanently high, which is the direction that pauses broker polling for good.
     * <p>
     * The collector runs on its own thread because the fixed version does its insertion inside the shard
     * map's own per-key lock, and a same-thread collection would be a recursive update on that map rather
     * than the race being modelled. Before the fix the collector completes immediately and the record lands
     * in the orphan; after it, the collector is simply blocked until the insertion has finished, which is the
     * whole point - so the join below is expected to time out on a fixed tree, and the collector is joined
     * properly at the end.
     */
    @Test
    void aRecordIsNeverAdmittedIntoAShardThatWasCollectedMidInsertion() throws InterruptedException {
        var keyOrdered = new PCModuleTestEnv(ParallelConsumerOptions.<String, String>builder()
                .ordering(KEY)
                .build());
        var keyOrderedWm = keyOrdered.workManager();
        var sm = keyOrderedWm.getSm();
        keyOrderedWm.onPartitionsAssigned(UniLists.of(TP));
        long epoch = keyOrderedWm.getPm().getEpochOfPartition(TP);

        var collector = new AtomicReference<Thread>();
        var armed = new AtomicBoolean(true);
        // THE INTERLEAVING: offset() is first read by ProcessingShard.addWorkContainer, i.e. after the
        // ShardManager has chosen the shard and before anything is written into it
        var record = new ConsumerRecord<String, String>(TOPIC, 0, 40L, "a-key", "a-value") {
            @Override
            public long offset() {
                if (armed.compareAndSet(true, false)) {
                    var thread = new Thread(() -> sm.removeShardIfEmpty(ShardKey.ofKey(this)), "shard-collector");
                    collector.set(thread);
                    thread.start();
                    ThreadUtils.joinQuietly(thread, Duration.ofMillis(500));
                }
                return super.offset();
            }
        };

        sm.addWorkContainer(epoch, record);

        Thread collectorThread = requireNonNull(collector.get(),
                "PRECONDITION: the seam must have fired, or this test exercises nothing");
        collectorThread.join(30_000);

        assertWithMessage("the admission must be in a shard the map still holds: one admitted into a "
                + "collected shard is unreachable forever and is never retired, so the figure the load "
                + "gate reads climbs and never comes back down")
                .that(sm.countRecordsInShardsByScan()).isEqualTo(sm.getNumberOfRecordsInShards());
        assertThat(sm.getNumberOfRecordsInShards()).isEqualTo(1L);
        assertWithMessage("and the record itself is still reachable as work")
                .that(sm.getWorkIfAvailable(10)).hasSize(1);
    }

    /**
     * Makes the collaborator run {@code action} the next time the shard asks it whether a container is stale,
     * and only then - the sweeps the action drives ask the same question themselves.
     */
    private void onNextStalenessCheck(PartitionStateManager<String, String> seam, Runnable action) {
        var armed = new AtomicBoolean(true);
        doAnswer(invocation -> {
            Object state = invocation.callRealMethod();
            if (armed.compareAndSet(true, false)) {
                action.run();
            }
            return state;
        }).when(seam).getPartitionState(ArgumentMatchers.<WorkContainer<String, String>>any());
    }

    private ProcessingShard<String, String> shardWith(PartitionStateManager<String, String> pm,
                                                      RecordPopulation population,
                                                      ConsumerRecord<String, String> record) {
        return new ProcessingShard<>(ShardKey.of(record, module.options().getOrdering()),
                module.options(), pm, population);
    }

    private ConsumerRecord<String, String> recordAt(long offset) {
        return new ConsumerRecord<>(TOPIC, 0, offset, "a-key", "value-" + offset);
    }

}
