package io.confluent.parallelconsumer.state;

import com.google.common.truth.Truth;
import io.confluent.csid.utils.TimeUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static io.confluent.parallelconsumer.ManagedTruth.assertThat;
import static one.util.streamex.LongStreamEx.range;

@Slf4j
@ExtendWith(MockitoExtension.class)
class PartitionStateTest {

    TopicPartition tp = new TopicPartition("myTopic", 0);

    PartitionState<String, String> state = new PartitionState<>(tp);

    @Mock
    ConsumerRecord<String, String> mockCr;

    private void injectWorkAtOffset(long offset) {
        WorkContainer<String, String> workContainer = createWorkFor(offset);
        state.addWorkContainer(workContainer);
    }

    private WorkContainer<String, String> createWorkFor(long offset) {
        WorkContainer<String, String> workContainer = new WorkContainer<>(0, mockCr, null, TimeUtils.getClock());
        Mockito.doReturn(offset).when(mockCr).offset();
        return workContainer;
    }

    @Test
    void thing() {
        int sequential = 3;
        long highestSucceeded = 10;
        long workQueued = 15;

        injectWork(workQueued);

        succeed(highestSucceeded);

        range(sequential + 1).forEach(this::succeed);

        assertThat(state).hasOffsetHighestSeen().isEqualTo(workQueued);
        assertThat(state).hasOffsetHighestSucceeded().isEqualTo(highestSucceeded);

        assertThat(state).hasOffsetHighestSequentialSucceeded().isEqualTo(sequential);
        assertThat(state).hasNextExpectedPolledOffset().isEqualTo(sequential + 1);

        assertThat(state).isDirty();

        assertThat(state).hasCommitDataIfDirtyPresent();

        // recursive truth generation not working
        OffsetAndMetadata offsetAndMetadata = state.getCommitDataIfDirty().get();
        long offset = offsetAndMetadata.offset();
        Truth.assertThat(offset).isEqualTo(sequential + 1);
        String metadata = offsetAndMetadata.metadata();
        Truth.assertThat(metadata).isNotEmpty();

        List<Long> incompletes = range(workQueued + 1)
                .without(highestSucceeded)
                .greater(sequential)
                .boxed().toList();
        assertThat(state).hasIncompleteOffsetsBelowHighestSucceeded().containsExactlyElementsIn(incompletes);
    }

    private void succeed(long offset) {
        state.onSuccess(createWorkFor(offset));
    }

    private void injectWork(long workQueued) {
        range(workQueued + 1).forEach(this::injectWorkAtOffset);
    }

}
