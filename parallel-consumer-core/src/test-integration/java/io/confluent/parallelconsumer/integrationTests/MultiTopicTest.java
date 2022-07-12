package io.confluent.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.hamcrest.Matchers;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static io.confluent.parallelconsumer.ManagedTruth.assertThat;
import static one.util.streamex.StreamEx.of;
import static org.awaitility.Awaitility.await;

/**
 * Originally created to investigate issue report #184
 *
 * @author Antony Stubbs
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
        int recordsPerTopic = 1;
        multiTopics.forEach(singleTopic -> sendMessages(singleTopic, recordsPerTopic));

        var pc = getKcu().buildPc(order);
        pc.subscribe(of(multiTopics).map(NewTopic::name).toList());

        AtomicInteger messageProcessedCount = new AtomicInteger();

        pc.poll(pollContext -> {
            log.debug(pollContext.toString());
            messageProcessedCount.incrementAndGet();
        });

        // processed
        int expectedMessagesCount = recordsPerTopic * numTopics;
        await().untilAtomic(messageProcessedCount, Matchers.is(equalTo(expectedMessagesCount)));

        // commits
        pc.requestCommitAsap();
//        pc.close();
        log.info("commit msg sent");

        //
        // old way with new consumer
//        Consumer<?, ?> assertingConsumer = kcu.createNewConsumer(false);
//        await().atMost(Duration.ofSeconds(10))
//                .untilAsserted(() -> {
//                    assertSeparateConsumerCommit(assertingConsumer, new HashSet<>(multiTopics), recordsPerTopic);
//                });

        // new way with facade
        await().atMost(Duration.ofSeconds(10))
                .failFast(pc::isClosedOrFailed)
                .untilAsserted(() -> {
                    assertCommit(pc, new HashSet<>(multiTopics), recordsPerTopic);
                });
        log.info("Offsets committed");
    }

    /**
     * Can't get committed offsets from PC wrapped consumer, so force commit by closing PC, then create new consumer
     * with same group id, and assert what offsets are told are committed.
     * <p>
     * When consumer-interface #XXX is merged, could just poll PC directly (see commented out assertCommit below).
     */
    private void assertSeparateConsumerCommit(Consumer<?, ?> assertingConsumer, HashSet<NewTopic> topics, int expectedOffset) {
        Set<TopicPartition> partitions = topics.stream().map(newTopic -> new TopicPartition(newTopic.name(), 0)).collect(Collectors.toSet());
        Map<TopicPartition, OffsetAndMetadata> committed = assertingConsumer.committed(partitions);
        var partitionSubjects = assertThat(assertingConsumer).hasCommittedToPartition(partitions);
        partitionSubjects.forEach((topicPartition, commitHistorySubject)
                -> commitHistorySubject.atLeastOffset(expectedOffset));
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
// depends on merge of features/consumer-interface branch
//    private void assertCommit(final ParallelEoSStreamProcessor<String, String> pc, NewTopic newTopic, int recordsPerTopic) {
//        var committer = getKcu().getLastConsumerConstructed();
//
//        assertThat(committer)
//                .hasCommittedToPartition(newTopic)
//                .offset(recordsPerTopic);
//    }

}
