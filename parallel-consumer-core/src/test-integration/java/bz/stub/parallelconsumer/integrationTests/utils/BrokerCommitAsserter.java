package bz.stub.parallelconsumer.integrationTests.utils;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import pl.tlinkowski.unij.api.UniSets;

import java.time.Duration;
import java.util.Set;

import static bz.stub.parallelconsumer.ManagedTruth.assertThat;
import static java.time.Duration.ofSeconds;
import static org.awaitility.Awaitility.await;

/**
 * Central point for asserting commits in integration tests against the brokers.
 *
 * @author Antony Stubbs
 */
@Slf4j
@RequiredArgsConstructor
public class BrokerCommitAsserter {

    @NonNull
    @Getter(AccessLevel.PRIVATE)
    private final String defaultTopic;

    @NonNull
    private final KafkaConsumer<?, ?> assertConsumer;

    public void assertConsumedAtLeastOffset(int target) {
        assertConsumedAtLeastOffset(getDefaultTopic(), target);
    }

    public void assertConsumedAtLeastOffset(String topic, int target) {
        setup(topic, target);

        await().untilAsserted(() -> {
            var poll = assertConsumer.poll(ofSeconds(1));

            log.debug("Polled {} records, looking for at least offset {}", poll.count(), target);
            assertThat(poll).hasHeadOffsetAtLeastInAnyTopicPartition(target);
        });

        post();
    }

    private void post() {
        assertConsumer.unsubscribe();
    }

    private void setup(String topic, int target) {
        log.debug("Asserting against topic: {}, expecting to consume at LEAST offset {}", topic, target);
        Set<String> topicSet = UniSets.of(topic);
        assertConsumer.subscribe(topicSet);
        assertConsumer.seekToBeginning(UniSets.of());
    }

    /**
     * Checks only once, after the given delay.
     * <p>
     * The delay is the window in which the records must NOT appear, so it belongs to the caller: it has to
     * be sized against whatever is holding them back, and must expire while that thing is still holding.
     * A delay that outlives the block turns this into an assertion about nothing - which is how
     * {@code TransactionTimeoutsTest#produceTimeout} flaked, when this method's own flat 5s raced the 5s
     * block it was checking.
     */
    public void assertConsumedAtMostOffset(String topic, int atMost, Duration delay) {
        setup(topic, atMost);

        log.debug("Delaying by {} to check consumption from topic {} by at most {}", delay, topic, atMost);
        await()
                .pollDelay(delay)
                .timeout(delay.plusSeconds(1))
                .untilAsserted(() -> {
                    var poll = assertConsumer.poll(ofSeconds(1));

                    log.debug("Polled {} records, looking for at MOST offset {}", poll.count(), atMost);
                    assertThat(poll).hasHeadOffsetAtMostInAnyTopicPartition(atMost);
                });

        post();
    }
}
