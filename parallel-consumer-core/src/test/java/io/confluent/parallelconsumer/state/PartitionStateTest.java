package io.confluent.parallelconsumer.state;

import io.confluent.csid.utils.TimeUtils;
import lombok.extern.slf4j.Slf4j;
import one.util.streamex.LongStreamEx;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@Slf4j
@ExtendWith(MockitoExtension.class)
public class PartitionStateTest {

    TopicPartition tp = new TopicPartition("myTopic", 0);

    PartitionState<String, String> state = new PartitionState<>(tp);

    @Mock
    ConsumerRecord<String, String> mockCr;

    private void injectWorkAtOffset(long offset) {
        WorkContainer<String, String> workContainer = new WorkContainer<>(0, mockCr, null, TimeUtils.getClock());
        Mockito.doReturn(offset).when(mockCr).offset();
        state.addWorkContainer(workContainer);
    }

    @Test
    void thing() {
        long highestSucceeded = 10;
        long workQueued = 15;
        injectWork(workQueued);

        Truth8.state.getIncompleteOffsetsBelowHighestSucceeded()
    }

    private void injectWork(long workQueued) {
        LongStreamEx.of(workQueued).forEach(this::injectWork);
    }

}
