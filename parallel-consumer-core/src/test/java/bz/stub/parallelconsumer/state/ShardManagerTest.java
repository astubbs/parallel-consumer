package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.internal.PCModule;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import bz.stub.parallelconsumer.offsets.OffsetMapCodecManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.threeten.extra.MutableClock;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.when;

/**
 * @author Antony Stubbs
 * @see ShardManager
 */
class ShardManagerTest {

    ModelUtils mu = new ModelUtils();
    PartitionState<String, String> state;
    WorkManager<String, String> wm;

    String topic = "myTopic";
    int partition = 0;

    TopicPartition tp = new TopicPartition(topic, partition);

    ConcurrentSkipListMap<Long, Optional<ConsumerRecord<String, String>>> incompleteOffsets = new ConcurrentSkipListMap<>();

    @BeforeEach
    void setup() {
        state = new PartitionState<>(0, mu.getModule(), tp, OffsetMapCodecManager.HighestOffsetAndIncompletes.of());
        wm = mu.getModule().workManager();
        wm.onPartitionsAssigned(UniLists.of(tp));
    }

    @Test
    void testAssignedQuickRevokeNPE() {
        // issue : https://github.com/confluentinc/parallel-consumer/issues/757
        // 1. partition assigned and incompleteOffsets existed
        // 2. right before begin to poll and process messages, it got revoked
        // 3. the processingShard has no data yet
        // 4. when revoked, try to delete entries with records from incompleteOffsets, no such record in entries
        PCModuleTestEnv module = mu.getModule();
        ShardManager<String, String> sm = new ShardManager<>(module, module.workManager());
        ConsumerRecord<String, String> consumerRecord = new ConsumerRecord<>(topic, partition, 1, null, "test1");

        Map<ShardKey, ProcessingShard<String, String>> processingShards = new ConcurrentHashMap<>();
        processingShards.put(ShardKey.ofKey(consumerRecord), new ProcessingShard<>(ShardKey.ofKey(consumerRecord), module.options(), wm.getPm(), sm.getRecordPopulation(), sm.getDispatchScanMeter()));
        sm.setProcessingShards(processingShards);
        incompleteOffsets.put(1L, Optional.of(consumerRecord));
        state.setIncompleteOffsets(incompleteOffsets);
        state.onPartitionsRemoved(sm);
        assertThat(sm.getShard(ShardKey.ofKey(consumerRecord))).isEmpty();
    }

    /**
     * {@link ProcessingShard#getWorkIfAvailable} carries its own stale-container sweep, for a container that went
     * stale without either epoch-change sweep having reached it. That only happens in a race, so it is driven
     * here directly rather than through {@link WorkManager}: the shard is deliberately not registered with a
     * {@link ShardManager}, which is what
     * {@link ShardManager#removeStaleContainers()} iterates.
     * <p>
     * It has to retire the record like every other departure. {@link RecordPopulation} has no clamp and nothing
     * reconciles it against the shards, so a removal path that forgets to retire leaks silently and throttles
     * record intake for the life of the consumer.
     */
    @Test
    void theInlineStaleSweepRetiresTheRecordItRemoves() {
        PCModuleTestEnv module = mu.getModule();
        var consumerRecord = new ConsumerRecord<>(topic, partition, 4L, "a-key", "a-value");

        var population = new RecordPopulation();
        var shard = new ProcessingShard<>(ShardKey.ofTopicPartition(consumerRecord), module.options(), wm.getPm(), population, new DispatchScanMeter());
        shard.addWorkContainer(new WorkContainer<>(wm.getPm().getEpochOfPartition(tp), consumerRecord, module));

        assertThat(population.getInSystem()).isEqualTo(1L);

        // the partition goes away, so what the shard is still holding is now stale
        wm.onPartitionsRevoked(UniLists.of(tp));

        var taken = shard.getWorkIfAvailable(10, new RetryQueue());

        assertThat(taken).isEmpty();
        assertWithMessage("the stale container was swept out of the shard")
                .that(shard.getCountOfWorkTracked()).isEqualTo(0L);
        assertWithMessage("and retired, so the conservation figure agrees with the empty shard")
                .that(population.getInSystem()).isEqualTo(0L);
        assertWithMessage("the available counter lands on exactly zero without needing a clamp")
                .that(shard.getCountOfWorkAwaitingSelection()).isEqualTo(0L);
    }

    @Test
    void retryQueueOrdering() {
        String topic = "topic";
        int partition = 0;

        PCModule<String, String> mockPcModule = mock(PCModule.class);
        MutableClock clock = MutableClock.of(Instant.now(), Clock.systemDefaultZone().getZone());
        when(mockPcModule.clock()).thenReturn(clock);
        when(mockPcModule.options()).thenReturn(ParallelConsumerOptions.<String, String>builder().build());
        RetryQueue retryQueue = new RetryQueue();

        WorkContainer<String, String> w0 = new WorkContainer<>(0, new ConsumerRecord<>(topic, partition, 0, "k", "v"), mockPcModule);

        WorkContainer<String, String> w1 = new WorkContainer<>(0, new ConsumerRecord<>(topic, partition, 1, "k", "v"), mockPcModule);
        WorkContainer<String, String> w2 = new WorkContainer<>(0, new ConsumerRecord<>(topic, partition, 2, "k", "v"), mockPcModule);
        WorkContainer<String, String> w3 = new WorkContainer<>(0, new ConsumerRecord<>(topic, partition, 3, "k", "v"), mockPcModule);


        retryQueue.add(w0);
        retryQueue.add(w1);
        retryQueue.add(w2);
        retryQueue.add(w3);

        assertThat(retryQueue.size()).isEqualTo(4);

        assertThat(w0).isNotEqualTo(w1);
        assertThat(w1).isNotEqualTo(w2);

        boolean removed = retryQueue.remove(w1);
        assertThat(removed).isTrue();
        assertThat(retryQueue.size()).isEqualTo(3);

        Assertions.assertThat(checkForNoDupes(retryQueue)).as("RetryQueue should not contain duplicates").isTrue();

        assertThat(retryQueue.contains(w0)).isTrue();
        assertThat(retryQueue.contains(w1)).isFalse();

        assertThat(retryQueue.contains(w0)).isTrue();
        assertThat(retryQueue.contains(w1)).isFalse();
        assertThat(retryQueue.contains(w2)).isTrue();
    }

    @Test
    void testRetryQueueOrdering() {
        RetryQueue retryQueue = new RetryQueue();
        PCModule<String, String> mockPcModule = mock(PCModule.class);
        MutableClock clock = MutableClock.of(Instant.now(), Clock.systemDefaultZone().getZone());
        when(mockPcModule.clock()).thenReturn(clock);
        when(mockPcModule.options()).thenReturn(ParallelConsumerOptions.<String, String>builder().build());

        String topic = "topic";
        int partition = 0;

        WorkContainer<String, String> wc1 = new WorkContainer<>(0, new ConsumerRecord<>(topic, partition, 0, "k", "v"), mockPcModule);
        wc1.onUserFunctionFailure(new Throwable("cause"));
        retryQueue.add(wc1);
        clock.add(10, ChronoUnit.SECONDS);
        WorkContainer<String, String> wc1_2 = new WorkContainer<>(0, new ConsumerRecord<>(topic, partition, 0, "k", "v"), mockPcModule);
        wc1_2.onUserFunctionFailure(new Throwable("cause"));
        retryQueue.add(wc1_2);
        Assertions.assertThat(retryQueue.size()).isEqualTo(1);
    }

    @Test
    void testRetryQueueOrderingMultipleTries() {
        String topic = "topic";
        int partition = 0;
        int retryTestNum = 0;
        while (retryTestNum < 5) {

            PCModule<String, String> mockPcModule = mock(PCModule.class);
            MutableClock clock = MutableClock.of(Instant.now(), Clock.systemDefaultZone().getZone());
            when(mockPcModule.clock()).thenReturn(clock);
            when(mockPcModule.options()).thenReturn(ParallelConsumerOptions.<String, String>builder().build());

            RetryQueue retryQueue = new RetryQueue();


            WorkContainer<String, String> w0 = new WorkContainer<>(
                    1, new ConsumerRecord<>(topic, partition, 0, "key0", "value0"), mockPcModule);
            w0.onUserFunctionFailure(new RuntimeException("test1"));
            retryQueue.add(w0);

            WorkContainer<String, String> w1 = new WorkContainer<>(
                    1, new ConsumerRecord<>(topic, partition, 1, "key1", "value0"), mockPcModule);
            // the retry ordering is keyed off the retry-due time, which the container reads from the
            // (mock) clock - so advance the clock directly instead of sleeping to make wall time move it
            clock.add(10, ChronoUnit.MILLIS);
            w1.onUserFunctionFailure(new RuntimeException("test2"));
            retryQueue.add(w1);

            WorkContainer<String, String> w2 = new WorkContainer<>(
                    1, new ConsumerRecord<>(topic, partition, 2, "key2", "value0"), mockPcModule);
            clock.add(10, ChronoUnit.MILLIS);
            w2.onUserFunctionFailure(new RuntimeException("test3"));
            retryQueue.add(w2);

            clock.add(10, ChronoUnit.MILLIS);
            w0.onUserFunctionFailure(new RuntimeException("a"));
            int tries = 0;
            while (retryQueue.size() < 4 && tries < 100) {
                clock.add(1, ChronoUnit.MILLIS);
                w0.onUserFunctionFailure(new RuntimeException("a"));
                retryQueue.add(w0);
                tries++;
            }
            // Sometimes 4 elements are observed in retryQueue
            Assertions.assertThat(retryQueue.size()).as("Expecting to have 3 elements").isEqualTo(3);

            retryQueue.remove(w0);
            retryQueue.remove(w1);
            retryQueue.remove(w2);
            Assertions.assertThat(retryQueue.size()).isEqualTo(0);
            retryTestNum++;
        }
    }

    private boolean checkForNoDupes(RetryQueue retryQueue) {
        Set<String> checkSet = new HashSet<>();
        try (RetryQueue.RetryQueueIterator retryQueueIterator = retryQueue.iterator()) {
            while (retryQueueIterator.hasNext()) {
                WorkContainer<?, ?> workContainer = retryQueueIterator.next();
                //Checking by topic + partition + offset for uniqueness
                if (!checkSet.add(workContainer.getTopicPartition().topic() + "_" + workContainer.getTopicPartition().partition() + "_" + workContainer.getCr().offset())) {
                    return false;
                }
            }
        }
        return true;
    }
}