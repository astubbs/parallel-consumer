package io.confluent.parallelconsumer.integrationTests.state;

/*-
 * Copyright (C) 2020-2026 Confluent, Inc.
 */

import io.confluent.csid.utils.ThreadUtils;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.PollContext;
import io.confluent.parallelconsumer.integrationTests.BrokerIntegrationTest;
import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils.GroupOption;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.ConsumerGroupState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * Deterministic reproduction of the {@code auto.offset.reset=latest} "nudge race" that made
 * {@code PartitionStateCommittedOffsetIT.committedOffsetRemoved[latest]} a long-standing cross-environment
 * CI flake (see {@code docs/solutions/test-flakiness/latest-reset-nudge-race-committedoffsetremoved-2026-07-30.md}).
 * <p>
 * The race: a consumer with LATEST reset and no committed offset resolves its start position at whatever
 * the log end is <b>when the reset executes</b>. If the test's only "nudge" record is produced before a
 * contention-delayed reset resolves, the consumer is positioned past every record that will ever exist,
 * and any await for "some record arrives" is unwinnable at any timeout.
 * <p>
 * This test <b>forces</b> the worst-case ordering instead of leaving it to load-luck: ALL records are
 * produced before the PC even starts, then we wait until the group is STABLE (join/sync/reset certainly
 * complete) and verify nothing was seen (the leapfrog, established deterministically). The await that
 * follows can therefore only succeed through the nudge-inside-the-await pattern
 * ({@link BrokerIntegrationTest#awaitWithTopicNudge}) - a plain await here reproduces the original
 * failure mode in seconds.
 */
@Timeout(120)
@Testcontainers
@Slf4j
class LatestResetTailNudgeIT extends BrokerIntegrationTest<String, String> {

    @BeforeEach
    void setup() {
        setupTopic();
    }

    @Test
    void latestResetConsumerLeapfroggedByPreProducedRecords_stillReceivesViaNudgedAwait() {
        // ALL records exist before the consumer is born - the guaranteed-worst-case ordering that load
        // contention only sometimes produces in the wild
        produceMessages(200);

        getKcu().setOffsetResetPolicy(OffsetResetStrategy.LATEST);
        Set<String> seenKeys = ConcurrentHashMap.newKeySet();
        ParallelEoSStreamProcessor<String, String> pc = getKcu().buildPc(UNORDERED, GroupOption.NEW_GROUP);
        String groupId = getKcu().getGroupId();
        pc.subscribe(of(getTopic()));
        pc.poll((PollContext<String, String> ctx) -> seenKeys.add(ctx.key()));

        try {
            // establish the leapfrog deterministically: group fully bootstrapped...
            await().atMost(30, SECONDS).untilAsserted(() -> {
                var group = getKcu().getAdmin().describeConsumerGroups(of(groupId)).all().get().get(groupId);
                assertWithMessage("group must reach STABLE (join/sync complete)")
                        .that(group.state()).isEqualTo(ConsumerGroupState.STABLE);
            });
            ThreadUtils.sleepSecondsLog(2); // grace: LATEST reset resolves within ms of sync - be certain

            // ...and, correctly per LATEST semantics, positioned past all 200 pre-produced records
            assertWithMessage("worst case established: LATEST reset resolved after all records existed, " +
                    "so nothing has been (or ever would be) received without a post-reset nudge")
                    .that(seenKeys).isEmpty();

            // THE PROPERTY UNDER TEST: an await that nudges the topic inside the loop is winnable from
            // this position; the old single-pre-await-nudge pattern is not (fails here in seconds).
            var nudges = new AtomicLong();
            awaitWithTopicNudge(pc, Duration.ofSeconds(1), Duration.ofSeconds(15), nudges, () ->
                    assertThat(seenKeys).isNotEmpty());

            log.info("Tail-positioned consumer received data after {} nudge(s)", nudges.get());
        } finally {
            pc.close();
        }
    }
}
