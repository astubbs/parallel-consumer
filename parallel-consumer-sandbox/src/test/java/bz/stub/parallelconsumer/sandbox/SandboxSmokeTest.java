package bz.stub.parallelconsumer.sandbox;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.fluent.ParallelConsumerInstance;
import bz.stub.parallelconsumer.fluent.Outcome;
import bz.stub.parallelconsumer.fluent.ParallelConsumerDefinition;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The end-to-end claim, and the first test written here: a two-route definition, unaltered from the shape a
 * broker would run, consumes driven records with no broker anywhere.
 *
 * <h2>Both halves of that claim, in one place</h2>
 * <b>The plumbing this module owns</b> - the feeds, encoding, offset seeding, assignment, pacing, the bound, the close -
 * shows up as driven records consumed and their offsets committed.
 * <p>
 * <b>And the dispatch above it</b>: each route's function ran, on its OWN topic's records, decoded into its own
 * type. That assertion was left for later when this test was first written, because the facade's dispatching
 * wrapper was not yet wired and {@code ParallelConsumerDefinition#dispatch} was a placeholder that completed
 * every record without running any function. It has been real since U3
 * ({@code processor.poll(dispatcher::dispatchWithoutProducing)}), and the instruction left here was to add the
 * assertion here rather than in a new file, so that the two halves stay together. This is that.
 * <p>
 * <b>Why counting per route matters and a key set does not.</b>
 * {@code SandboxConsumer}'s constructor pre-fills its offset map with zero for every topic and partition, so
 * {@code publishedCounts().keySet()} is fixed before a record exists: it says which topics the definition routes,
 * and nothing whatever about whether either of them carried a record. With the record bound counting across all
 * topics, a run in which the second feed published nothing would have passed.
 */
@Timeout(60)
class SandboxSmokeTest {

    private static final int RECORD_BOUND = 20;

    private static final String ORDERS_TOPIC = "orders";

    private static final String DISPATCHES_TOPIC = "dispatches";

    private static final TopicPartition ORDERS_0 = new TopicPartition(ORDERS_TOPIC, 0);

    private static final TopicPartition DISPATCHES_0 = new TopicPartition(DISPATCHES_TOPIC, 0);

    @Test
    void aTwoRouteDefinitionConsumesGeneratedRecordsWithNoBroker() {
        // What each route's own function saw, kept apart so that "both routes ran" is a fact rather than a total.
        ConcurrentLinkedQueue<String> ordersSeen = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<String> dispatchesSeen = new ConcurrentLinkedQueue<>();

        ParallelConsumerDefinition definition = SandboxFixtures.definition();
        definition.string(ORDERS_TOPIC).process(context -> {
            ordersSeen.add(context.value());
            return Outcome.succeeded();
        });
        definition.string(DISPATCHES_TOPIC).process(context -> {
            dispatchesSeen.add(context.value());
            return Outcome.succeeded();
        });

        Sandbox sandbox = Sandbox.builder()
                // Fast enough that the test is not a stopwatch, slow enough that the pacing code is still the
                // thing being exercised rather than a tight loop.
                .perSecond(500)
                .bound(Bound.afterRecords(RECORD_BOUND))
                // Each topic's value names its own topic, which is what makes "each route saw its OWN records"
                // an assertion rather than a count.
                .feeding(ORDERS_TOPIC, SandboxFixtures.countedValues(ORDERS_TOPIC))
                .feeding(DISPATCHES_TOPIC, SandboxFixtures.countedValues(DISPATCHES_TOPIC))
                .build();

        try (ParallelConsumerInstance instance = definition.start(sandbox)) {
            assertWithMessage("the record bound should have been reached and the instance closed")
                    .that(sandbox.awaitBound(Duration.ofSeconds(30))).isTrue();
            instance.awaitShutdown();
        }

        assertThat(sandbox.drivenRecords()).isEqualTo(RECORD_BOUND);
        assertWithMessage("the driver reads the definition, so a topic no route claims is a topic nothing is "
                + "driven for")
                .that(sandbox.consumer().publishedCounts().keySet())
                .containsExactly(ORDERS_0, DISPATCHES_0);
        // ...and the key set alone says only that, so each topic's count is asserted separately.
        assertThat(sandbox.consumer().publishedCounts().get(ORDERS_0)).isGreaterThan(0L);
        assertThat(sandbox.consumer().publishedCounts().get(DISPATCHES_0)).isGreaterThan(0L);

        long committed = totalCommittedOffsets(sandbox);
        assertWithMessage("every driven record should have been consumed and its offset committed; the "
                + "sandbox published %s", sandbox.consumer().publishedCounts())
                .that(committed).isEqualTo(RECORD_BOUND);

        assertRoutesRanOnTheirOwnRecords(ordersSeen, dispatchesSeen);
    }

    /**
     * The half the placeholder wrapper could not support: each route's function ran, and what it was handed was
     * its own topic's records, decoded - not the other route's, and not raw bytes.
     */
    private static void assertRoutesRanOnTheirOwnRecords(ConcurrentLinkedQueue<String> ordersSeen,
                                                         ConcurrentLinkedQueue<String> dispatchesSeen) {
        assertWithMessage("the orders route's own function should have run").that(ordersSeen).isNotEmpty();
        assertWithMessage("the dispatches route's own function should have run").that(dispatchesSeen).isNotEmpty();
        assertWithMessage("between them the two functions saw every record the bound published")
                .that(ordersSeen.size() + dispatchesSeen.size()).isEqualTo(RECORD_BOUND);

        // Each value names the topic its own feed published it to, so a route handed the OTHER route's records -
        // which is what a feed wired to the wrong publisher would do - fails here rather than passing the counts.
        for (String order : ordersSeen) {
            assertThat(order).startsWith(ORDERS_TOPIC + "-");
        }
        for (String dispatch : dispatchesSeen) {
            assertThat(dispatch).startsWith(DISPATCHES_TOPIC + "-");
        }
    }

    /**
     * The sum of the last committed offset for every partition - which, for a run that started at offset zero
     * and committed everything, is the number of records consumed.
     */
    private static long totalCommittedOffsets(Sandbox sandbox) {
        long total = 0;
        for (TopicPartition partition : sandbox.consumer().publishedCounts().keySet()) {
            total += SandboxFixtures.highestCommittedOffset(sandbox, partition);
        }
        return total;
    }
}
