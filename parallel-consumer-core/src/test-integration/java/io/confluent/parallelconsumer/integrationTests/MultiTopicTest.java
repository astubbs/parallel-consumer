
/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */
package io.confluent.parallelconsumer.integrationTests;

import io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.state.ShardKey;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.hamcrest.Matchers;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static io.confluent.parallelconsumer.ManagedTruth.assertThat;
import static one.util.streamex.StreamEx.of;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;

/**
 * Originally created to investigate issue report #184
 *
 * @see ShardKey
 * @see io.confluent.parallelconsumer.state.ShardKeyTest
 */
@Slf4j
class MultiTopicTest extends BrokerIntegrationTest<String, String> {

    //    @SneakyThrows
    @ParameterizedTest
    @EnumSource(ProcessingOrder.class)
    void multiTopic(ProcessingOrder order) {
        // possible to remove dependency on consumer facade?

        int numTopics = 3;
        List<NewTopic> multiTopics = getKcu().createTopics(numTopics);
        int recordsPerTopic = 10;
        multiTopics.forEach(singleTopic -> sendMessages(singleTopic, recordsPerTopic));

        var pc = getKcu().buildPc(order);
        pc.subscribe(of(multiTopics).map(NewTopic::name).toList());

        AtomicInteger messageProcessedCount = new AtomicInteger();

        pc.poll(pollContext -> {
            log.debug(pollContext.toString());
            messageProcessedCount.incrementAndGet();
        });

        // processed
        var integer = recordsPerTopic * numTopics;
        await().untilAtomic(messageProcessedCount, Matchers.is(equalTo(integer)));

        // commits
        pc.requestCommitAsap();
        log.info("commit msg sent");

        //
        await().atMost(Duration.ofSeconds(10))
                .failFast(pc::isClosedOrFailed)
                .untilAsserted(() -> {
                    assertCommit(pc, new HashSet<>(multiTopics), recordsPerTopic);
//                    multiTopics.forEach(singleTopic -> assertCommit(pc, singleTopic, recordsPerTopic));
                });
        log.info("Offsets committed");
    }


    @SneakyThrows
    private void sendMessages(NewTopic newTopic, int recordsPerTopic) {
        getKcu().produceMessages(newTopic.name(), recordsPerTopic);
    }

    private void assertCommit(final ParallelEoSStreamProcessor<String, String> pc, NewTopic newTopic, int expectedOffset) {
        log.error("Current check: topic {} committed {}",
                newTopic.name(),
                pc.getConsumerFacade().committed(new TopicPartition(newTopic.name(), 0)));
        assertThat(pc)
                .hasCommittedToAnyAssignedPartitionOf(newTopic)
                .offset(expectedOffset);
    }

    private void assertCommit(final ParallelEoSStreamProcessor<String, String> pc, Set<NewTopic> newTopic, int expectedOffset) {
//        log.error("Current check: topic {} committed {}",
//                newTopic,
//                pc.getConsumerFacade().committed(newTopic));

        var partitionSubjects = assertThat(pc).hasCommittedToAnyAssignedPartitionOf(newTopic);
        partitionSubjects.forEach((topicPartition, commitHistorySubject) -> commitHistorySubject.atLeastOffset(expectedOffset));
    }

}
