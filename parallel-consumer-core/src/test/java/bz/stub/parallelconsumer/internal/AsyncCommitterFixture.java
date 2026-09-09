package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.state.WorkManager;
import lombok.Value;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * The setup shared by the unit tests that drive {@link ConsumerOffsetCommitter}'s asynchronous branch by hand: a
 * committer in {@link bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode#PERIODIC_CONSUMER_ASYNCHRONOUS}
 * mode over a mocked {@link ConsumerManager}, and the requests it handed to that client.
 * <p>
 * Shared rather than written out per test class because the whole point of these tests is to answer a commit at a
 * moment of their choosing, so every one of them needs the same two things - a mocked {@link ConsumerManager} to
 * capture the request and its callback from, and an async-mode committer. Copying that between classes is what the
 * duplication gates flag, and it is the shape {@code PCModuleTestEnv}'s {@code withHandDrivenProcessor} was
 * extracted for on the same grounds. What goes on the other side varies: a bare {@link WorkManager} mock where the
 * success marking is nobody's business, and a real one where the point is what the acknowledgement does to a
 * partition.
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
     * The requests the committer handed to {@code commitAsync}, in send order - each one its offsets and its
     * callback, kept together so a test can answer whichever it likes, whenever it likes. Answering them out of
     * order is the only way to reach the overlapping-commit paths at all.
     *
     * @param expectedSends how many {@code commitAsync} calls the committer should have made by now; asserted
     *                      rather than assumed, so a test that fails to send its second commit reports that
     *                      instead of an index out of bounds
     */
    static List<AsyncCommitRequest> commitsHandedToTheClient(ConsumerManager<String, String> consumerMgr,
                                                             int expectedSends) {
        ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> offsets = offsetMapCaptor();
        var callback = ArgumentCaptor.forClass(OffsetCommitCallback.class);
        verify(consumerMgr, times(expectedSends)).commitAsync(offsets.capture(), callback.capture());

        List<AsyncCommitRequest> requests = new ArrayList<>();
        for (int send = 0; send < expectedSends; send++) {
            requests.add(new AsyncCommitRequest(offsets.getAllValues().get(send), callback.getAllValues().get(send)));
        }
        return requests;
    }

    @SuppressWarnings("unchecked")
    static ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> offsetMapCaptor() {
        return ArgumentCaptor.forClass((Class<Map<TopicPartition, OffsetAndMetadata>>) (Class<?>) Map.class);
    }

    /**
     * One {@code commitAsync} the committer sent: what it asked the broker to commit, and the callback the broker's
     * answer arrives through.
     */
    @Value
    static class AsyncCommitRequest {

        Map<TopicPartition, OffsetAndMetadata> offsets;

        OffsetCommitCallback callback;

        /**
         * Answers this request as the client's response thread would.
         *
         * @param exception {@code null} for the broker acknowledging the commit
         */
        void answerWith(Exception exception) {
            callback.onComplete(offsets, exception);
        }

        /**
         * @return the offset asked for on one partition, so a test can say what reached the broker rather than
         * assuming it
         */
        long offsetFor(TopicPartition topicPartition) {
            OffsetAndMetadata offered = offsets.get(topicPartition);
            assertWithMessage("commit request %s carried nothing for %s", offsets, topicPartition)
                    .that(offered).isNotNull();
            return offered.offset();
        }
    }

}
