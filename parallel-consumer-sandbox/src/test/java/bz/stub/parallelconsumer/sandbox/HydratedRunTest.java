package bz.stub.parallelconsumer.sandbox;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.fluent.ConsumerHandle;
import bz.stub.parallelconsumer.fluent.Outcome;
import bz.stub.parallelconsumer.fluent.ParallelConsumerDefinition;
import bz.stub.parallelconsumer.sandbox.demo.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The hydration through a whole run: a route's function is handed realistic values of its own declared type,
 * having gone out through that route's serialiser and come back through its deserialiser.
 *
 * <h2>Why this is not covered by {@code HydrationTest}</h2>
 * That file asks the hydration directly for an object and asserts on what it made - which is the right place for
 * "an {@code email} field holds an email address", and the wrong place for everything between the filler and the
 * function. A value can be filled perfectly and still not survive the round trip: the fluent path encodes it with
 * the route's serialiser because the engine underneath consumes raw bytes, and a field the route's JSON mapping
 * cannot write, or cannot read back, turns up here as an empty one. Nothing else asserts on the pair.
 *
 * <h2>Why it is not in the driver's own tests either</h2>
 * The driver's tests - the bound, the parked run, the smoke test, key partitioning - are fed values written by
 * hand, because none of them is about the data and a test that says what it publishes is a better test. This is
 * the one that <em>is</em> about the data, so it is the one that lets the hydration fill it.
 */
@Timeout(60)
class HydratedRunTest {

    private static final String ORDERS = "orders";

    private static final int RECORDS = 20;

    @Test
    void aRoutesFunctionIsHandedRealisticValuesOfItsOwnTypeAfterARoundTripThroughItsFormat() {
        ConcurrentLinkedQueue<Order> seen = new ConcurrentLinkedQueue<>();
        ParallelConsumerDefinition definition = SandboxFixtures.definition();
        definition.json(ORDERS, Order.class).process(context -> {
            seen.add(context.value());
            return Outcome.succeeded();
        });

        Sandbox sandbox = Sandbox.builder()
                .perSecond(1000)
                .bound(Bound.afterRecords(RECORDS))
                .seed(7)
                .build();

        try (ConsumerHandle handle = definition.start(sandbox)) {
            assertThat(sandbox.awaitBound(Duration.ofSeconds(30))).isTrue();
            handle.awaitShutdown();
        }

        assertThat(seen).hasSize(RECORDS);
        Order order = seen.peek();
        assertWithMessage("a route handed an empty instance would satisfy the count above, so the assertion has "
                + "to be about a field the hydration filled")
                .that(order.getOrderId()).isNotEmpty();
        assertWithMessage("the field-name rules are what make this an email rather than a random string, and they "
                + "have to survive the route's own encode and decode to be worth anything")
                .that(order.getEmail()).contains("@");
        assertThat(order.getCustomerName()).isNotEmpty();
    }

    /**
     * The classic path, where <b>nothing is encoded at all</b>: a mock consumer holds records of the instance's
     * own types, so the function is handed the very object the hydration made. That is the difference from the
     * fluent path, and asserting it here is what stops a green fluent run from being read as covering both.
     */
    @Test
    void aClassicInstanceIsHandedTheFilledObjectsThemselves() {
        ConcurrentLinkedQueue<Order> seen = new ConcurrentLinkedQueue<>();
        Sandbox sandbox = Sandbox.builder()
                .perSecond(1000)
                .bound(Bound.afterRecords(RECORDS))
                .build();

        try (ClassicSandbox<String, Order> classic = sandbox.classic(String.class, Order.class, ORDERS)) {
            ParallelEoSStreamProcessor<String, Order> pc = SandboxFixtures.startClassic(classic,
                    SandboxFixtures.partitionOrdered(classic),
                    context -> seen.add(context.getSingleRecord().value()));

            assertThat(classic.awaitBound(Duration.ofSeconds(30))).isTrue();
            pc.closeDrainFirst();
        }

        assertThat(seen).hasSize(RECORDS);
        Order order = seen.peek();
        assertThat(order.getEmail()).contains("@");
        assertThat(order.getCustomerName()).isNotEmpty();
    }
}
