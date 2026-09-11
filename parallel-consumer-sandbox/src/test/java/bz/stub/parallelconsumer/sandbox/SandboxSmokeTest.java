package bz.stub.parallelconsumer.sandbox;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.fluent.ConsumerHandle;
import bz.stub.parallelconsumer.fluent.Outcome;
import bz.stub.parallelconsumer.fluent.ParallelConsumerDefinition;
import bz.stub.parallelconsumer.sandbox.demo.Dispatch;
import bz.stub.parallelconsumer.sandbox.demo.Order;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The end-to-end claim, and the first test written here: a two-route definition, unaltered from the shape a
 * broker would run, consumes generated records with no broker anywhere.
 *
 * <h2>Both halves of that claim, in one place</h2>
 * <b>The plumbing this module owns</b> - generation, encoding, seeding, assignment, pacing, the bound, the close -
 * shows up as generated records consumed and their offsets committed.
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
        ConcurrentLinkedQueue<Order> ordersSeen = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Dispatch> dispatchesSeen = new ConcurrentLinkedQueue<>();

        ParallelConsumerDefinition definition = SandboxFixtures.definition();
        definition.json(ORDERS_TOPIC, Order.class).process(context -> {
            ordersSeen.add(context.value());
            return Outcome.succeeded();
        });
        definition.json(DISPATCHES_TOPIC, Dispatch.class).process(context -> {
            dispatchesSeen.add(context.value());
            return Outcome.succeeded();
        });

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
        assertWithMessage("the generator reads the definition, so a topic no route claims is a topic nothing is "
                + "generated for")
                .that(sandbox.consumer().publishedCounts().keySet())
                .containsExactly(ORDERS_0, DISPATCHES_0);
        // ...and the key set alone says only that, so each topic's count is asserted separately.
        assertThat(sandbox.consumer().publishedCounts().get(ORDERS_0)).isGreaterThan(0L);
        assertThat(sandbox.consumer().publishedCounts().get(DISPATCHES_0)).isGreaterThan(0L);

        long committed = totalCommittedOffsets(sandbox);
        assertWithMessage("every generated record should have been consumed and its offset committed; the "
                + "sandbox published %s", sandbox.consumer().publishedCounts())
                .that(committed).isEqualTo(RECORD_BOUND);

        assertRoutesRanOnTheirOwnRecords(ordersSeen, dispatchesSeen);
    }

    /**
     * The half the placeholder wrapper could not support: each route's function ran, and what it was handed was
     * its own topic's records decoded into its own type - not the other route's, and not raw bytes.
     */
    private static void assertRoutesRanOnTheirOwnRecords(ConcurrentLinkedQueue<Order> ordersSeen,
                                                         ConcurrentLinkedQueue<Dispatch> dispatchesSeen) {
        assertWithMessage("the orders route's own function should have run").that(ordersSeen).isNotEmpty();
        assertWithMessage("the dispatches route's own function should have run").that(dispatchesSeen).isNotEmpty();
        assertWithMessage("between them the two functions saw every record the bound generated")
                .that(ordersSeen.size() + dispatchesSeen.size()).isEqualTo(RECORD_BOUND);

        // Decoded, and filled - a route handed an empty instance would satisfy the counts above.
        Order order = ordersSeen.peek();
        assertThat(order.getOrderId()).isNotEmpty();
        assertThat(order.getEmail()).contains("@");
        Dispatch dispatch = dispatchesSeen.peek();
        assertThat(dispatch.getDispatchId()).isNotEmpty();
        assertThat(dispatch.getDepotCity()).isNotEmpty();
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
