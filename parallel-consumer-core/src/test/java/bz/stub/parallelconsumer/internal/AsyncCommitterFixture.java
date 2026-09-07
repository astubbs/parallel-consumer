package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.state.WorkManager;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * The setup shared by the unit tests that drive {@link ConsumerOffsetCommitter}'s asynchronous branch by hand:
 * a committer wired to two mocks, and the {@link OffsetCommitCallback}s it handed to the client.
 * <p>
 * Shared rather than written out per test class because the whole point of these tests is to answer a commit at a
 * moment of their choosing, so every one of them needs the same three things - a mocked {@link ConsumerManager} to
 * capture the callback from, a mocked {@link WorkManager} to observe the success marking on, and a committer in
 * {@link bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode#PERIODIC_CONSUMER_ASYNCHRONOUS} mode. Copying
 * that between classes is what the duplication gates flag, and it is the shape {@code PCModuleTestEnv}'s
 * {@code withHandDrivenProcessor} was extracted for on the same grounds.
 * <p>
 * Not named {@code *Test}, so surefire does not collect it - it holds no test methods.
 *
 * @author Antony Stubbs
 */
final class AsyncCommitterFixture {

    /**
     * Consumer commits ignore the group metadata - only the transactional producer path uses it - so every caller
     * here can hand over the same one.
     */
    static final ConsumerGroupMetadata GROUP = new ConsumerGroupMetadata("a-group");

    private AsyncCommitterFixture() {
    }

    @SuppressWarnings("unchecked")
    static ConsumerManager<String, String> consumerManagerMock() {
        return mock(ConsumerManager.class);
    }

    @SuppressWarnings("unchecked")
    static WorkManager<String, String> workManagerMock() {
        return mock(WorkManager.class);
    }

    /**
     * {@code build()} does not validate - {@code validate()} is what needs a real consumer, and nothing here calls
     * it.
     */
    static ConsumerOffsetCommitter<String, String> asyncCommitter(ConsumerManager<String, String> consumerMgr,
                                                                 WorkManager<String, String> workManager) {
        var options = ParallelConsumerOptions.<String, String>builder()
                .commitMode(PERIODIC_CONSUMER_ASYNCHRONOUS)
                .build();
        return new ConsumerOffsetCommitter<>(consumerMgr, workManager, options);
    }

    /**
     * The callbacks the committer handed to {@code commitAsync}, in send order, so a test can complete them in any
     * order it likes - which is the only way to reach the superseded-answer paths.
     *
     * @param expectedSends how many {@code commitAsync} calls the committer should have made by now; asserted
     *                      rather than assumed, so a test that fails to send its second commit reports that
     *                      instead of an index out of bounds
     */
    static List<OffsetCommitCallback> callbacksHandedToTheClient(ConsumerManager<String, String> consumerMgr,
                                                                int expectedSends) {
        var callback = ArgumentCaptor.forClass(OffsetCommitCallback.class);
        verify(consumerMgr, times(expectedSends)).commitAsync(anyMap(), callback.capture());
        return callback.getAllValues();
    }

    /**
     * A one-partition commit on {@code topic}, so a test can name the offset it is asserting about.
     */
    static Map<TopicPartition, OffsetAndMetadata> commitOf(String topic, long offset, String metadata) {
        // singletonMap rather than Map.of: --release 8 restricts the API surface (see the Jabel note in
        // the root pom), so the Java 9 factory does not compile here
        return Collections.singletonMap(partitionOf(topic, 0), new OffsetAndMetadata(offset, metadata));
    }

    /**
     * A two-partition commit, so a test can send a newer request that supersedes ONE of the partitions and leaves
     * the other's offset where it was - which is the only shape that distinguishes a per-partition rule from a
     * whole-request one.
     */
    static Map<TopicPartition, OffsetAndMetadata> commitOf(String topic,
                                                           long offsetOnPartition0,
                                                           long offsetOnPartition1,
                                                           String metadata) {
        Map<TopicPartition, OffsetAndMetadata> commit = new HashMap<>();
        commit.put(partitionOf(topic, 0), new OffsetAndMetadata(offsetOnPartition0, metadata));
        commit.put(partitionOf(topic, 1), new OffsetAndMetadata(offsetOnPartition1, metadata));
        return commit;
    }

    static TopicPartition partitionOf(String topic, int partition) {
        return new TopicPartition(topic, partition);
    }

}
