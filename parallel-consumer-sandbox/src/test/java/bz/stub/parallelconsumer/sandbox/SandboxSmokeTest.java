package bz.stub.parallelconsumer.sandbox;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumer;
import bz.stub.parallelconsumer.fluent.ConsumerHandle;
import bz.stub.parallelconsumer.fluent.Outcome;
import bz.stub.parallelconsumer.fluent.ParallelConsumerDefinition;
import bz.stub.parallelconsumer.sandbox.demo.Dispatch;
import bz.stub.parallelconsumer.sandbox.demo.Order;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The end-to-end claim, and the first test written here: a two-route definition, unaltered from the shape a
 * broker would run, consumes generated records with no broker anywhere.
 *
 * <h2>What it can assert today, and what it cannot yet</h2>
 * The facade's dispatching wrapper - decode, run the route's function, map its outcome - is a separate unit and
 * is not wired at the commit this test was written against: {@code ParallelConsumerDefinition#dispatch} is a
 * placeholder that completes every record <em>without</em> running any route's function, and says so at warn.
 * <p>
 * So this asserts what the placeholder allows and no more: <b>generated records were consumed and their offsets
 * committed</b>, which is the whole of the plumbing this module owns - generation, encoding, seeding, assignment,
 * pacing, the bound, the close. <b>The assertion U6 strengthens</b> is the one this cannot make: that each route's
 * function saw its own topic's records, decoded into its own type. When the wrapper lands, add it here rather
 * than in a new file, so the two halves of "the sandbox runs a definition" stay in one place.
 */
@Slf4j
@Timeout(60)
class SandboxSmokeTest {

    private static final int RECORD_BOUND = 20;

    @Test
    void aTwoRouteDefinitionConsumesGeneratedRecordsWithNoBroker() {
        ParallelConsumerDefinition definition = ParallelConsumer.connect(new Properties());
        definition.json("orders", Order.class)
                .process(context -> Outcome.succeeded());
        definition.json("dispatches", Dispatch.class)
                .process(context -> Outcome.succeeded());

        Sandbox sandbox = Sandbox.builder()
                // Fast enough that the test is not a stopwatch, slow enough that the pacing code is still the
                // thing being exercised rather than a tight loop.
                .perSecond(500)
                .bound(Bound.afterRecords(RECORD_BOUND))
                .seed(7)
                .build();

        try (ConsumerHandle handle = definition.start(sandbox)) {
            assertWithMessage("the record bound should have been reached and the instance closed")
                    .that(sandbox.awaitBound(Duration.ofSeconds(30))).isTrue();
            handle.awaitShutdown();
        }

        assertThat(sandbox.generatedRecords()).isEqualTo(RECORD_BOUND);
        assertThat(sandbox.consumer().publishedCounts().keySet())
                .containsExactly(new TopicPartition("orders", 0), new TopicPartition("dispatches", 0));

        long committed = totalCommittedOffsets(sandbox);
        assertWithMessage("every generated record should have been consumed and its offset committed; the "
                + "sandbox published %s", sandbox.consumer().publishedCounts())
                .that(committed).isEqualTo(RECORD_BOUND);
    }

    /**
     * The sum of the last committed offset for every partition - which, for a run that started at offset zero
     * and committed everything, is the number of records consumed.
     */
    private static long totalCommittedOffsets(Sandbox sandbox) {
        long total = 0;
        for (Map<TopicPartition, OffsetAndMetadata> commit : sandbox.consumer().getCommitHistoryInt()) {
            // Later commits supersede earlier ones for the same partition, so only the last matters - but the
            // histories are per-call, so walk them and keep the highest per partition.
            log.debug("Commit: {}", commit);
        }
        for (TopicPartition partition : sandbox.consumer().publishedCounts().keySet()) {
            long highest = 0;
            for (Map<TopicPartition, OffsetAndMetadata> commit : sandbox.consumer().getCommitHistoryInt()) {
                OffsetAndMetadata offset = commit.get(partition);
                if (offset != null) {
                    highest = Math.max(highest, offset.offset());
                }
            }
            total += highest;
        }
        return total;
    }
}
